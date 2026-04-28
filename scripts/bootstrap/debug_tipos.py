import httpx
import json
from api.config import settings

login_url = f"{settings.SEI_BASE_URL}/orgaos/usuarios/login"
payload = {"Usuario": "gabriel.coelho@sead.pi.gov.br", "Senha": "fenek3161@SEAD", "Orgao": "SEAD-PI"}
headers = {"accept": "application/json", "Content-Type": "application/json"}

response = httpx.post(login_url, json=payload, headers=headers, verify=False, timeout=30)
data = response.json()
token = data.get("Token")
id_unidade = data["Unidades"][0].get("Id")

print(f"Token OK: {token[:30]}...")
print(f"Unit: {id_unidade}")

# Buscar tipos
url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/documentos/tipos"
headers = {"accept": "application/json", "token": token}
params = {"limit": 100}

print(f"Fetching {url}")
response = httpx.get(url, headers=headers, params=params, verify=False, timeout=300)
print(f"Status: {response.status_code}")
data = response.json()
print(f"Type: {type(data)}")
print(f"Keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
print(f"Content:\n{json.dumps(data, indent=2)[:1000]}")
