#!/usr/bin/env python3
"""
Debug script for WSSEI API (mod-wssei).
Tests /autenticar and discovers the correct orgao ID by trying values 0-10.
"""
import httpx
import json
import sys

BASE_URL = "https://sei.pi.gov.br/sei/modulos/wssei/controlador_ws.php/api/v2"
USER = "gabriel.coelho@sead.pi.gov.br"
PASS = "fenek3161@SEAD"


def pp(label, data):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    if isinstance(data, (dict, list)):
        print(json.dumps(data, indent=2, ensure_ascii=False))
    else:
        print(data)


def try_login(client: httpx.Client, orgao: int) -> tuple[int, dict | str]:
    r = client.post(
        f"{BASE_URL}/autenticar",
        data={"usuario": USER, "senha": PASS, "orgao": orgao},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    try:
        body = r.json()
    except Exception:
        body = r.text[:2000]
    return r.status_code, body


def listar_orgaos(client: httpx.Client, token: str):
    r = client.get(
        f"{BASE_URL}/orgao/listar",
        headers={"token": token},
        timeout=30,
    )
    try:
        body = r.json()
    except Exception:
        body = r.text[:3000]
    pp(f"GET /orgao/listar — {r.status_code}", body)
    return body


def main():
    max_orgao = int(sys.argv[1]) if len(sys.argv) > 1 else 10

    with httpx.Client(verify=True) as client:
        print(f"Testing /autenticar with orgao IDs 0..{max_orgao}")
        print(f"User: {USER}")
        print()

        found_token = None
        found_orgao = None

        for orgao_id in range(0, max_orgao + 1):
            status, body = try_login(client, orgao_id)
            success = isinstance(body, dict) and body.get("sucesso") is True

            if success:
                token = body.get("data", {}).get("token", "")
                print(f"  orgao={orgao_id}  ->  {status}  SUCCESS  token={token[:30]}...")
                if not found_token:
                    found_token = token
                    found_orgao = orgao_id
            else:
                msg = ""
                if isinstance(body, dict):
                    msg = body.get("mensagem", body.get("message", ""))
                print(f"  orgao={orgao_id}  ->  {status}  FAIL  {msg}")

        if found_token:
            pp(f"LOGIN OK — orgao={found_orgao}", {"orgao": found_orgao, "token": found_token})
            print("\nFetching /orgao/listar to show all available orgs...")
            listar_orgaos(client, found_token)
        else:
            print(f"\nNo successful login found for orgao 0..{max_orgao}")
            print("Try a higher range:  python debug_wssei.py 50")


if __name__ == "__main__":
    main()
