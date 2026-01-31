import os
import requests
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("NAV_API_TOKEN")
headers = {"Authorization": f"Bearer {token}"}
base_url = "https://pam-stilling-feed.nav.no/api/v1/feed"

def probe(params):
    print(f"Probing {params}...")
    try:
        r = requests.get(base_url, headers=headers, params=params)
        print(f"Status: {r.status_code}")
        data = r.json()
        items = data.get("items", [])
        if items:
            first = items[0].get("date_modified")
            print(f"First item date: {first}")
        else:
            print("No items.")
    except Exception as e:
        print(e)

# Try common date parameter names
probe({"date": "2025-11-01"})
probe({"updated": "2025-11-01"})
probe({"modified": "2025-11-01"})
probe({"since": "2025-11-01T00:00:00"})
