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
"""

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx
from neo4j import GraphDatabase

# ── Config ──────────────────────────────────────────────────────────────────

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password123")

SEI_BASE_URL = os.getenv("SEI_BASE_URL", "https://api.sei.pi.gov.br/v1")

RETRY_MAX = 3
RETRY_BACKOFF = 2  # seconds
HTTP_TIMEOUT = 180  # seconds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Neo4j queries ───────────────────────────────────────────────────────────

QUERY_PROCESSOS_SEAD = """
MATCH (p:Processo)-[:PASSOU_PELO_ORGAO]->(o:Orgao {sigla: $orgao})
OPTIONAL MATCH (p)-[:CONTEM_DOCUMENTO]->(d:Documento)
OPTIONAL MATCH (p)-[:PASSOU_PELA_UNIDADE]->(u:Unidade)
WITH p, collect(DISTINCT d) AS docs, collect(DISTINCT u) AS unidades
RETURN p.protocolo_formatado AS protocolo,
       [d IN docs | {numero: d.numero, tipo: d.tipo}] AS documentos,
       [u IN unidades | {sigla: u.sigla, id_unidade: u.id_unidade}] AS unidades
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


def download_document(
    client: httpx.Client,
    token: str,
    id_unidade: str,
    documento_numero: str,
) -> dict | None:
    """Download a single document from SEI. Returns {filename, content_bytes, tipo} or None."""
    url = f"{SEI_BASE_URL}/unidades/{id_unidade}/documentos/baixar"
    headers = {"accept": "application/json", "token": token}
    params = {"protocolo_documento": documento_numero}

    try:
        resp = _sei_request(client, url, headers, params)
    except Exception as e:
        log.error("Request failed for doc %s unidade %s: %s", documento_numero, id_unidade, e)
        return None

    if resp.status_code != 200:
        log.debug(
            "HTTP %d for doc %s unidade %s: %s",
            resp.status_code, documento_numero, id_unidade, resp.text[:200],
        )
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


def try_download_with_unidades(
    client: httpx.Client,
    token: str,
    documento_numero: str,
    unidades: list[dict],
) -> tuple[dict | None, str | None]:
    """Try downloading a document using each unidade until one succeeds.
    Returns (result, id_unidade_used) or (None, None).
    """
    for u in unidades:
        id_unidade = u.get("id_unidade")
        if not id_unidade:
            continue
        result = download_document(client, token, str(id_unidade), documento_numero)
        if result is not None:
            return result, str(id_unidade)
    return None, None


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

def fetch_processos_from_neo4j(orgao: str) -> list[dict]:
    """Query Neo4j for processos that passed through the given orgão."""
    log.info("Connecting to Neo4j: %s", NEO4J_URI)
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    log.info("Neo4j connected")

    processos = []
    with driver.session() as session:
        result = session.run(QUERY_PROCESSOS_SEAD, orgao=orgao)
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
    driver.close()
    return processos


def download_processo_docs(
    processo: dict,
    token: str,
    output_dir: Path,
    dry_run: bool = False,
) -> dict:
    """Download all documents for a single processo. Returns stats."""
    protocolo = processo["protocolo"]
    documentos = processo["documentos"]
    unidades = processo["unidades"]

    stats = {"total": len(documentos), "downloaded": 0, "failed": 0, "skipped": 0}

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
                log.debug("Already downloaded: %s/%s", protocolo, numero)
                stats["skipped"] += 1
                continue

            result, used_unidade = try_download_with_unidades(client, token, numero, unidades)

            if result is None:
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
    parser.add_argument("--dry-run", action="store_true", help="Only list what would be downloaded")
    parser.add_argument("--workers", type=int, default=3, help="Parallel processo workers (default: 3)")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of processos (0=all)")
    args = parser.parse_args()

    # Resolve SEI token (login if needed)
    token = resolve_token(args)

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Phase 1: Query Neo4j
    processos = fetch_processos_from_neo4j(args.orgao)
    if args.limit > 0:
        processos = processos[:args.limit]

    total_docs = sum(len(p["documentos"]) for p in processos)
    log.info(
        "Found %d processos with %d documents that passed through %s",
        len(processos), total_docs, args.orgao,
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
    totals = {"total": 0, "downloaded": 0, "failed": 0, "skipped": 0}
    completed = 0

    def _do_download(processo):
        return download_processo_docs(processo, token, output_dir)

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
                    "[%d/%d] Processo %s: %d downloaded, %d failed, %d skipped",
                    completed, len(processos), protocolo,
                    stats["downloaded"], stats["failed"], stats["skipped"],
                )
            except Exception as e:
                log.error("[%d/%d] Processo %s ERROR: %s", completed, len(processos), protocolo, e)

    log.info(
        "DONE. Total: %d docs | Downloaded: %d | Failed: %d | Skipped: %d",
        totals["total"], totals["downloaded"], totals["failed"], totals["skipped"],
    )


if __name__ == "__main__":
    main()
