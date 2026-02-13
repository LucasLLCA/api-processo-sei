#!/usr/bin/env python3
"""
Debug script to test SEI API endpoints directly.
Shows full response payloads for debugging 422/500 errors.
"""
import httpx
import json
import sys

BASE_URL = "https://api.sei.pi.gov.br/v1"
CREDENTIALS = {
    "Usuario": "gabriel.coelho@sead.pi.gov.br",
    "Senha": "fenek3161@SEAD",
    "Orgao": "SEAD-PI",
}


def pp(label: str, data):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    if isinstance(data, (dict, list)):
        print(json.dumps(data, indent=2, ensure_ascii=False))
    else:
        print(data)


def login(client: httpx.Client) -> dict:
    print("Logging in...")
    r = client.post(
        f"{BASE_URL}/orgaos/usuarios/login",
        json=CREDENTIALS,
        headers={"accept": "application/json", "Content-Type": "application/json"},
        timeout=30,
    )
    pp(f"POST /login — {r.status_code}", r.text[:3000])
    r.raise_for_status()
    return r.json()


def consulta(client: httpx.Client, token: str, id_unidade: str, protocolo: str):
    url = f"{BASE_URL}/unidades/{id_unidade}/procedimentos/consulta"
    params = {
        "protocolo_procedimento": protocolo,
        "sinal_unidades_procedimento_aberto": "S",
        "sinal_completo": "N",
        "sinal_assuntos": "N",
        "sinal_interessados": "N",
        "sinal_observacoes": "N",
        "sinal_andamento_geracao": "N",
        "sinal_andamento_conclusao": "N",
        "sinal_ultimo_andamento": "N",
        "sinal_procedimentos_relacionados": "N",
        "sinal_procedimentos_anexados": "N",
    }
    headers = {"accept": "application/json", "token": f'"{token}"'}

    print(f"\nGET {url}")
    print(f"  params: {json.dumps(params, indent=2)}")
    print(f"  headers: {headers}")

    r = client.get(url, params=params, headers=headers, timeout=45)
    try:
        body = r.json()
    except Exception:
        body = r.text[:3000]
    pp(f"GET /consulta — {r.status_code}", body)
    return r.status_code, body


def andamentos(client: httpx.Client, token: str, id_unidade: str, protocolo: str):
    url = f"{BASE_URL}/unidades/{id_unidade}/procedimentos/andamentos"
    params = {
        "protocolo_procedimento": protocolo,
        "limite_registros": "10",
        "pagina_atual": "1",
    }
    headers = {"accept": "application/json", "token": f'"{token}"'}

    print(f"\nGET {url}")
    r = client.get(url, params=params, headers=headers, timeout=45)
    try:
        body = r.json()
    except Exception:
        body = r.text[:3000]
    pp(f"GET /andamentos — {r.status_code}", body)
    return r.status_code, body


def documentos(client: httpx.Client, token: str, id_unidade: str, protocolo: str):
    url = f"{BASE_URL}/unidades/{id_unidade}/procedimentos/documentos"
    params = {
        "protocolo_procedimento": protocolo,
        "limite_registros": "5",
        "pagina_atual": "1",
    }
    headers = {"accept": "application/json", "token": f'"{token}"'}

    print(f"\nGET {url}")
    r = client.get(url, params=params, headers=headers, timeout=45)
    try:
        body = r.json()
    except Exception:
        body = r.text[:3000]
    pp(f"GET /documentos — {r.status_code}", body)
    return r.status_code, body


def main():
    if len(sys.argv) < 3:
        print(f"Usage: python {sys.argv[0]} <protocolo> <id_unidade> [endpoint]")
        print(f"  endpoint: consulta (default), andamentos, documentos, all")
        print(f"\nExample:")
        print(f"  python {sys.argv[0]} 00012023358202538 110006613")
        print(f"  python {sys.argv[0]} 00012023358202538 110006613 all")
        sys.exit(1)

    protocolo = sys.argv[1]
    id_unidade = sys.argv[2]
    endpoint = sys.argv[3] if len(sys.argv) > 3 else "consulta"

    with httpx.Client() as client:
        login_data = login(client)
        token = login_data.get("Token", "")
        if not token:
            print("ERROR: No token in login response!")
            sys.exit(1)

        print(f"\nToken: {token[:20]}...")
        units = login_data.get("Unidades", [])
        if units:
            pp("Available units", [{"IdUnidade": u.get("IdUnidade"), "Sigla": u.get("Sigla"), "Descricao": u.get("Descricao")} for u in units])

        if endpoint in ("consulta", "all"):
            consulta(client, token, id_unidade, protocolo)
        if endpoint in ("andamentos", "all"):
            andamentos(client, token, id_unidade, protocolo)
        if endpoint in ("documentos", "all"):
            documentos(client, token, id_unidade, protocolo)


if __name__ == "__main__":
    main()
