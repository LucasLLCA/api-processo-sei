import asyncio
import httpx
import json
from api.config import settings

async def test_login():
    url = f"{settings.SEI_BASE_URL}/orgaos/usuarios/login"
    payload = {
        "Usuario": "gabriel.coelho@sead.pi.gov.br",
        "Senha": "fenek3161@SEAD",
        "Orgao": "SEAD-PI"
    }
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    
    async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
        response = await client.post(url, json=payload, headers=headers)
    
    print(f"Status: {response.status_code}")
    print(f"Response:\n{json.dumps(response.json(), indent=2)}")

asyncio.run(test_login())
