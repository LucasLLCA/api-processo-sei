"""
Download all documents from processos that passed through orgão SEAD-PI.

Strategy:
  1. Query Neo4j for processos with relationship PASSOU_PELO_ORGAO → Orgao{sigla:"SEAD-PI"}
  2. For each processo, collect Documento nodes (via CONTEM_DOCUMENTO) and
     Unidade nodes (via PASSOU_PELA_UNIDADE) to get candidate id_unidade values
  3. For each document, attempt download via SEI API trying each unidade until success
  4. Save files as: <output_dir>/<protocolo_formatado>/<documento_numero>/<filename>

Authentication (pick one):
  --id-pessoa ID         Auto-login using stored credentials from PostgreSQL
  --usuario U --senha S  Direct login with SEI credentials
  --token TOKEN          Use an existing SEI API token

Usage:
    python scripts/download_documentos_sead.py --id-pessoa 12345
    python scripts/download_documentos_sead.py --usuario user --senha pass
    python scripts/download_documentos_sead.py --token SEI_TOKEN
    python scripts/download_documentos_sead.py --id-pessoa 12345 --output ./documentos
    python scripts/download_documentos_sead.py --id-pessoa 12345 --dry-run
    python scripts/download_documentos_sead.py --id-pessoa 12345 --orgao "SEAD-PI" --workers 5
    python scripts/download_documentos_sead.py --id-pessoa 12345 --processo "00019.000123/2025-01"
    python scripts/download_documentos_sead.py --id-pessoa 12345 --processo "00019.000123/2025-01" "00019.000456/2025-02"
"""

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx

from pipeline.cli import add_standard_args, resolve_settings
from pipeline.config import ConfigError, Settings
from pipeline.logging_setup import configure_logging
from pipeline.neo4j_driver import build_driver, run_with_retry

# ── Config ──────────────────────────────────────────────────────────────────

SEI_BASE_URL = os.getenv("SEI_BASE_URL", "https://api.sei.pi.gov.br/v1")

RETRY_MAX = 3
RETRY_BACKOFF = 2  # seconds
HTTP_TIMEOUT = 180  # seconds

log = configure_logging(__name__)

# ── Neo4j queries ───────────────────────────────────────────────────────────

QUERY_PROCESSOS_BY_ORGAO = """
MATCH (p:Processo)-[:PASSOU_PELO_ORGAO]->(o:Orgao {sigla: $orgao})
OPTIONAL MATCH (p)-[:CONTEM_DOCUMENTO]->(d:Documento)
OPTIONAL MATCH (p)-[:PASSOU_PELA_UNIDADE]->(u:Unidade)
WITH p, collect(DISTINCT d) AS docs, collect(DISTINCT u) AS unidades
RETURN p.protocolo_formatado AS protocolo,
       [d IN docs | {numero: d.numero, tipo: d.tipo}] AS documentos,
       [u IN unidades | {sigla: u.sigla, id_unidade: u.id_unidade}] AS unidades
"""

QUERY_PROCESSOS_BY_PROTOCOLO = """
MATCH (p:Processo)
WHERE p.protocolo_formatado IN $protocolos
OPTIONAL MATCH (p)-[:CONTEM_DOCUMENTO]->(d:Documento)
OPTIONAL MATCH (p)-[:PASSOU_PELA_UNIDADE]->(u:Unidade)
WITH p, collect(DISTINCT d) AS docs, collect(DISTINCT u) AS unidades
RETURN p.protocolo_formatado AS protocolo,
       [d IN docs | {numero: d.numero, tipo: d.tipo}] AS documentos,
       [u IN unidades | {sigla: u.sigla, id_unidade: u.id_unidade}] AS unidades
"""

# Query: which unidade created each document in a processo
QUERY_DOC_CREATORS = """
MATCH (p:Processo {protocolo_formatado: $protocolo})-[:CONTEM_DOCUMENTO]->(d:Documento)
OPTIONAL MATCH (a:Atividade)-[:REFERENCIA_DOCUMENTO]->(d)
WHERE a.tipo_acao IN ['GERACAO-DOCUMENTO', 'ARQUIVO-ANEXADO', 'RECEBIMENTO-DOCUMENTO']
OPTIONAL MATCH (a)-[:EXECUTADO_PELA_UNIDADE]->(u:Unidade)
RETURN d.numero AS doc_numero, u.sigla AS unidade_sigla, u.id_unidade AS id_unidade
"""


# ── SEI API helpers (synchronous) ──────────────────────────────────────────

def _sei_request(client: httpx.Client, url: str, headers: dict, params: dict) -> httpx.Response:
    """HTTP GET with retry logic matching the async version in sei.py."""
    for attempt in range(RETRY_MAX):
        try:
            resp = client.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
            return resp
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            if attempt < RETRY_MAX - 1:
                wait = RETRY_BACKOFF * (attempt + 1)
                log.warning("Retry %d/%d after %s: %s", attempt + 1, RETRY_MAX, type(e).__name__, e)
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("unreachable")


class _DocumentCancelled(Exception):
    """Raised when SEI reports a document as cancelled — no point trying other unidades."""
    pass


class _AccessDenied(Exception):
    """Raised when the unidade does not have access — try the next unidade."""
    pass


def download_document(
    client: httpx.Client,
    token: str,
    id_unidade: str,
    documento_numero: str,
) -> dict | None:
    """Download a single document from SEI.

    Returns {filename, content_bytes, tipo} on success.
    Raises _DocumentCancelled if the doc is permanently unavailable.
    Raises _AccessDenied if this unidade can't access it (try another).
    Returns None on other failures.
    """
    url = f"{SEI_BASE_URL}/unidades/{id_unidade}/documentos/baixar"
    headers = {"accept": "application/json", "token": token}
    params = {"protocolo_documento": documento_numero}

    try:
        resp = _sei_request(client, url, headers, params)
    except Exception as e:
        log.error("Request failed for doc %s unidade %s: %s", documento_numero, id_unidade, e)
        return None

    if resp.status_code != 200:
        body = resp.text[:300]
        log.warning(
            "HTTP %d for doc %s unidade %s: %s",
            resp.status_code, documento_numero, id_unidade, body,
        )
        # Document-level: cancelled — will fail for every unidade
        if "foi cancelado" in body:
            raise _DocumentCancelled(f"Documento {documento_numero} foi cancelado")
        # Unidade-level: no access — try next unidade
        if "não possui acesso" in body or "Acesso negado" in body:
            raise _AccessDenied(body)
        return None

    content_disposition = resp.headers.get("content-disposition", "")
    import re
    match = re.search(r'filename="(.+)"', content_disposition)
    filename = match.group(1) if match else f"documento_{documento_numero}.bin"

    return {
        "filename": filename,
        "content": resp.content,
        "tipo": "pdf" if filename.lower().endswith(".pdf") else "html",
    }


def _sort_unidades(
    unidades: list[dict],
    login_orgao: str,
    doc_creator_unidade: dict | None = None,
    success_stats: dict | None = None,
) -> list[dict]:
    """Filter and sort unidades by likelihood of download success.

    Only keeps unidades from the login orgão (external ones are skipped).
    Priority order:
      1. Document creator unidade (if known and belongs to login orgão)
      2. Login orgão unidades, ranked by historical success
    """
    login_orgao_prefix = login_orgao + "/"  # e.g. "SEAD-PI/"

    def _is_own_orgao(sigla: str) -> bool:
        return sigla == login_orgao or sigla.startswith(login_orgao_prefix)

    # Filter: only keep unidades from login orgão
    own_unidades = [u for u in unidades if _is_own_orgao(u.get("sigla", ""))]

    # Include doc creator if it belongs to login orgão and isn't already in the list
    if doc_creator_unidade and _is_own_orgao(doc_creator_unidade.get("sigla", "")):
        creator_id = str(doc_creator_unidade.get("id_unidade", ""))
        if not any(str(u.get("id_unidade", "")) == creator_id for u in own_unidades):
            own_unidades.append(doc_creator_unidade)

    def _sort_key(u):
        sigla = u.get("sigla", "")
        id_unidade = str(u.get("id_unidade", ""))

        # Priority 0: document creator
        if doc_creator_unidade and id_unidade == str(doc_creator_unidade.get("id_unidade", "")):
            return (0, 0)

        # Priority 1: ranked by historical success count (higher = better)
        hist_score = -(success_stats.get(sigla, 0)) if success_stats else 0
        return (1, hist_score)

    return sorted(own_unidades, key=_sort_key)


def try_download_with_unidades(
    client: httpx.Client,
    token: str,
    documento_numero: str,
    unidades: list[dict],
    login_orgao: str = "",
    doc_creator_unidade: dict | None = None,
    success_stats: dict | None = None,
) -> tuple[dict | None, str | None, str]:
    """Try downloading a document using each unidade until one succeeds.
    Returns (result, id_unidade_used, status).
    status is one of: "ok", "cancelled", "failed".

    Unidades are sorted by likelihood of success before trying.
    """
    sorted_unidades = _sort_unidades(unidades, login_orgao, doc_creator_unidade, success_stats)

    tried = []
    for u in sorted_unidades:
        id_unidade = u.get("id_unidade")
        sigla = u.get("sigla", "?")
        if not id_unidade:
            continue
        log.info("  Trying doc %s with unidade %s (id=%s)", documento_numero, sigla, id_unidade)
        try:
            result = download_document(client, token, str(id_unidade), documento_numero)
        except _DocumentCancelled:
            log.warning("  ✗ Doc %s is CANCELLED — skipping remaining unidades", documento_numero)
            return None, None, "cancelled"
        except _AccessDenied:
            tried.append(sigla)
            continue
        if result is not None:
            log.info("  ✓ Downloaded doc %s via unidade %s", documento_numero, sigla)
            # Update success stats for future docs in this run
            if success_stats is not None:
                success_stats[sigla] = success_stats.get(sigla, 0) + 1
            return result, str(id_unidade), "ok"
        tried.append(sigla)
    log.warning(
        "  ✗ All %d unidades failed for doc %s: %s",
        len(tried), documento_numero, ", ".join(tried),
    )
    return None, None, "failed"


# ── SEI Login ─────────────────────────────────────────────────────────────

def sei_login(usuario: str, senha: str, orgao: str) -> str:
    """Login to SEI API and return the authentication token."""
    url = f"{SEI_BASE_URL}/orgaos/usuarios/login"
    with httpx.Client(verify=False) as client:
        for attempt in range(RETRY_MAX):
            try:
                resp = client.post(
                    url,
                    headers={"accept": "application/json", "Content-Type": "application/json"},
                    json={"Usuario": usuario, "Senha": senha, "Orgao": orgao},
                    timeout=30,
                )
                break
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                if attempt < RETRY_MAX - 1:
                    log.warning("Login retry %d/%d: %s", attempt + 1, RETRY_MAX, e)
                    time.sleep(RETRY_BACKOFF * (attempt + 1))
                else:
                    log.error("SEI login failed after %d attempts: %s", RETRY_MAX, e)
                    sys.exit(1)

    if resp.status_code == 401:
        log.error("SEI login failed: invalid credentials (usuario=%s, orgao=%s)", usuario, orgao)
        sys.exit(1)
    if resp.status_code != 200:
        log.error("SEI login failed: HTTP %d — %s", resp.status_code, resp.text[:500])
        sys.exit(1)

    data = resp.json()
    token = data.get("Token")
    if not token:
        log.error("SEI login response missing 'Token': %s", list(data.keys()))
        sys.exit(1)

    log.info("SEI login successful (usuario=%s, orgao=%s)", usuario, orgao)
    return token


def autologin_from_db(id_pessoa: int) -> str:
    """Fetch stored credentials from PostgreSQL, decrypt password, and login to SEI."""
    import psycopg2
    from cryptography.fernet import Fernet
    from dotenv import dotenv_values

    # Load config from api-processo-sei/.env (resolve relative to this script)
    env_path = Path(__file__).resolve().parent.parent / ".env"
    env = dotenv_values(env_path)

    db_host = env.get("DATABASE_HOST", os.getenv("DATABASE_HOST", "localhost"))
    db_port = env.get("DATABASE_PORT", os.getenv("DATABASE_PORT", "5432"))
    db_user = env.get("DATABASE_USER", os.getenv("DATABASE_USER", "postgres"))
    db_pass = env.get("DATABASE_PASSWORD", os.getenv("DATABASE_PASSWORD", ""))
    db_name = env.get("DATABASE_NAME", os.getenv("DATABASE_NAME", "postgres"))
    fernet_key = env.get("FERNET_KEY", os.getenv("FERNET_KEY", ""))

    if not fernet_key:
        log.error("FERNET_KEY env var is required for auto-login")
        sys.exit(1)

    log.info("Fetching stored credentials for id_pessoa=%d", id_pessoa)
    conn = psycopg2.connect(
        host=db_host, port=int(db_port), user=db_user, password=db_pass, dbname=db_name,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT usuario_sei, senha_encrypted, orgao "
                "FROM credenciais_usuario "
                "WHERE id_pessoa = %s AND deletado_em IS NULL",
                (id_pessoa,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if row is None:
        log.error("No stored credentials found for id_pessoa=%d", id_pessoa)
        sys.exit(1)

    usuario_sei, senha_encrypted, orgao = row
    f = Fernet(fernet_key.encode())
    try:
        senha = f.decrypt(senha_encrypted.encode()).decode()
    except Exception as e:
        log.error("Failed to decrypt password for id_pessoa=%d: %s", id_pessoa, e)
        sys.exit(1)

    log.info("Auto-login: id_pessoa=%d, usuario=%s, orgao=%s", id_pessoa, usuario_sei, orgao)
    return sei_login(usuario_sei, senha, orgao)


def resolve_token(args) -> str:
    """Resolve SEI token from CLI args: --token, --id-pessoa, or --usuario/--senha."""
    if args.token:
        return args.token
    if args.id_pessoa:
        return autologin_from_db(args.id_pessoa)
    if args.usuario and args.senha:
        return sei_login(args.usuario, args.senha, args.orgao)
    log.error("Authentication required: use --token, --id-pessoa, or --usuario/--senha")
    sys.exit(1)


# ── Main logic ──────────────────────────────────────────────────────────────

def fetch_processos_from_neo4j(
    settings: Settings,
    orgao: str,
    protocolos: list[str] | None = None,
) -> list[dict]:
    """Query Neo4j for processos by orgão or by specific protocolos.

    Also fetches per-document creator unidade for smart ordering.

    NOTE: this function currently requires a live Neo4j connection.
    `--read-json` is a known gap here — the Documento node + CONTEM_DOCUMENTO
    edge are emitted via the composite `load_documento` template by
    `etl_neo4j.py`, so reconstructing them from an emit directory would
    require decomposing that template into clean node/edge writes first.
    Tracked as a follow-up.
    """
    log.info("Connecting to Neo4j: %s", settings.neo4j_uri)
    driver = build_driver(settings)
    log.info("Neo4j connected")

    processos: list[dict] = []
    try:
        with driver.session() as session:
            if protocolos:
                log.info("Querying %d specific processos", len(protocolos))
                result = session.run(QUERY_PROCESSOS_BY_PROTOCOLO, protocolos=protocolos)
            else:
                log.info("Querying processos for orgão: %s", orgao)
                result = session.run(QUERY_PROCESSOS_BY_ORGAO, orgao=orgao)
            for record in result:
                protocolo = record["protocolo"]
                documentos = record["documentos"]
                unidades = record["unidades"]
                # Filter out null documents (OPTIONAL MATCH can return null properties)
                documentos = [d for d in documentos if d.get("numero")]
                unidades = [u for u in unidades if u.get("id_unidade")]
                if documentos:
                    processos.append({
                        "protocolo": protocolo,
                        "documentos": documentos,
                        "unidades": unidades,
                    })

            # Phase 2: fetch document creator unidades for smart ordering
            log.info("Fetching document creator unidades for %d processos...", len(processos))
            for p in processos:
                doc_creators: dict = {}  # doc_numero → {sigla, id_unidade}
                result = session.run(QUERY_DOC_CREATORS, protocolo=p["protocolo"])
                for rec in result:
                    doc_num = rec["doc_numero"]
                    if doc_num and rec["id_unidade"]:
                        doc_creators[doc_num] = {
                            "sigla": rec["unidade_sigla"],
                            "id_unidade": rec["id_unidade"],
                        }
                p["doc_creators"] = doc_creators
    finally:
        driver.close()

    return processos


def download_processo_docs(
    processo: dict,
    token: str,
    output_dir: Path,
    login_orgao: str = "",
    success_stats: dict | None = None,
    dry_run: bool = False,
) -> dict:
    """Download all documents for a single processo. Returns stats."""
    protocolo = processo["protocolo"]
    documentos = processo["documentos"]
    unidades = processo["unidades"]
    doc_creators = processo.get("doc_creators", {})

    stats = {"total": len(documentos), "downloaded": 0, "failed": 0, "skipped": 0, "cancelled": 0}

    if not unidades:
        log.warning("Processo %s has no unidades — skipping %d docs", protocolo, len(documentos))
        stats["skipped"] = len(documentos)
        return stats

    processo_dir = output_dir / protocolo.replace("/", "_")

    if dry_run:
        log.info(
            "[DRY-RUN] Processo %s: %d documentos, %d unidades to try",
            protocolo, len(documentos), len(unidades),
        )
        return stats

    with httpx.Client(verify=False) as client:
        for doc in documentos:
            numero = doc["numero"]
            doc_dir = processo_dir / numero

            # Skip if already downloaded
            if doc_dir.exists() and any(doc_dir.iterdir()):
                log.info("  Already on disk, skipping: %s/%s", protocolo, numero)
                stats["skipped"] += 1
                continue

            # Get creator unidade for this specific document (if known)
            creator = doc_creators.get(numero)

            result, used_unidade, status = try_download_with_unidades(
                client, token, numero, unidades,
                login_orgao=login_orgao,
                doc_creator_unidade=creator,
                success_stats=success_stats,
            )

            if result is None:
                if status == "cancelled":
                    stats["cancelled"] += 1
                else:
                    log.warning("FAILED doc %s in processo %s (tried %d unidades)", numero, protocolo, len(unidades))
                    stats["failed"] += 1
                continue

            doc_dir.mkdir(parents=True, exist_ok=True)
            filepath = doc_dir / result["filename"]
            filepath.write_bytes(result["content"])
            stats["downloaded"] += 1
            log.debug(
                "Downloaded %s → %s (unidade=%s, %d bytes)",
                numero, filepath, used_unidade, len(result["content"]),
            )

    return stats


def main():
    parser = argparse.ArgumentParser(description="Download SEAD-PI processo documents from SEI")

    # Authentication (pick one)
    auth = parser.add_argument_group("authentication (pick one)")
    auth.add_argument("--token", help="SEI API token")
    auth.add_argument("--id-pessoa", type=int, help="Auto-login using stored credentials for this id_pessoa")
    auth.add_argument("--usuario", help="SEI username for direct login")
    auth.add_argument("--senha", help="SEI password for direct login")

    parser.add_argument("--output", default="./documentos_sead", help="Output directory (default: ./documentos_sead)")
    parser.add_argument("--orgao", default="SEAD-PI", help="Orgão sigla to filter (default: SEAD-PI)")
    parser.add_argument("--processo", nargs="+", default=None, help="Specific protocolo(s) to download (overrides --orgao)")
    parser.add_argument("--dry-run", action="store_true", help="Only list what would be downloaded")
    parser.add_argument("--workers", type=int, default=3, help="Parallel processo workers (default: 3)")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of processos (0=all)")
    # --read-json is not meaningful for this script yet (the Documento
    # graph lives inside composite templates in the emit dir, not as clean
    # nodes/edges); --workers is owned by this script.
    add_standard_args(parser, skip={"--read-json", "--workers"})
    args = parser.parse_args()

    settings = resolve_settings(args)
    configure_logging(__name__, settings.log_level)

    # Resolve SEI token (login if needed)
    token = resolve_token(args)

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Phase 1: Query Neo4j
    try:
        processos = fetch_processos_from_neo4j(settings, args.orgao, args.processo)
    except ConfigError as e:
        log.error("%s", e)
        sys.exit(2)
    if args.limit > 0:
        processos = processos[:args.limit]

    total_docs = sum(len(p["documentos"]) for p in processos)
    filter_desc = f"protocolos {args.processo}" if args.processo else f"orgão {args.orgao}"
    log.info(
        "Found %d processos with %d documents (%s)",
        len(processos), total_docs, filter_desc,
    )

    if not processos:
        log.info("Nothing to download.")
        return

    if args.dry_run:
        for p in processos:
            log.info(
                "  %s: %d docs, unidades: %s",
                p["protocolo"],
                len(p["documentos"]),
                ", ".join(u["sigla"] for u in p["unidades"]),
            )
        log.info("DRY-RUN complete. Use without --dry-run to download.")
        return

    # Phase 2: Download documents
    # Shared success stats — learns which unidades work best during the run
    success_stats: dict[str, int] = {}
    totals = {"total": 0, "downloaded": 0, "failed": 0, "skipped": 0, "cancelled": 0}
    completed = 0

    log.info("Unidade priority: 1) doc creator, 2) %s unidades, 3) others", args.orgao)

    def _do_download(processo):
        return download_processo_docs(
            processo, token, output_dir,
            login_orgao=args.orgao,
            success_stats=success_stats,
        )

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_do_download, p): p["protocolo"] for p in processos}
        for future in as_completed(futures):
            protocolo = futures[future]
            completed += 1
            try:
                stats = future.result()
                for k in totals:
                    totals[k] += stats[k]
                log.info(
                    "[%d/%d] Processo %s: %d downloaded, %d failed, %d cancelled, %d skipped",
                    completed, len(processos), protocolo,
                    stats["downloaded"], stats["failed"], stats["cancelled"], stats["skipped"],
                )
            except Exception as e:
                log.error("[%d/%d] Processo %s ERROR: %s", completed, len(processos), protocolo, e)

    log.info(
        "DONE. Total: %d docs | Downloaded: %d | Failed: %d | Cancelled: %d | Skipped: %d",
        totals["total"], totals["downloaded"], totals["failed"], totals["cancelled"], totals["skipped"],
    )


if __name__ == "__main__":
    main()
