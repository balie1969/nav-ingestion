import os
import json
import logging
from dotenv import load_dotenv
from nav_client import NavClient

logging.basicConfig(level=logging.INFO)

def check_last_page():
    load_dotenv()
    api_token = os.getenv("NAV_API_TOKEN")
    client = NavClient(api_token=api_token)

    print("Fetching LAST feed page...")
    page = client.get_feed_page(last=True)
    
    print(f"Page ID: {page.get('id')}")
    print(f"Next ID: {page.get('next_id')}")
    print(f"Next URL: {page.get('next_url')}")
    print(f"Item count: {len(page.get('items', []))}")

if __name__ == "__main__":
    check_last_page()
