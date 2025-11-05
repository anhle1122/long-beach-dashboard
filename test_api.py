import os
import requests
from dotenv import load_dotenv

print("🔧 Loading .env ...")
load_dotenv()

api_key = os.getenv("CENSUS_API_KEY")
print("✅ API key loaded?" , "YES" if api_key else "NO (None)")
print("API key (first 6 chars):", (api_key[:6] + "…") if api_key else "None")

url = (
    "https://api.census.gov/data/2023/acs/acs5/profile"
    "?get=NAME,DP05_0001E,DP03_0062E"
    "&for=place:43000&in=state:06"
    f"&key={api_key}"
)
print("🌐 Requesting:", url)

try:
    r = requests.get(url, timeout=30)
    print("📦 Status Code:", r.status_code)
    r.raise_for_status()
    data = r.json()
    print("🟩 First row (header):", data[0])
    print("🟩 Second row (values):", data[1])
except Exception as e:
    print("❌ Error:", repr(e))
    print("Response text (first 500 chars):")
    try:
        print(r.text[:500])
    except:
        print("(no response)")