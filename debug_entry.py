import os
import json
import logging
from dotenv import load_dotenv
from nav_client import NavClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_single_entry():
    load_dotenv()
    api_token = os.getenv("NAV_API_TOKEN")
    client = NavClient(api_token=api_token)

    # Fetch LAST page to get newest items
    logger.info("Fetching LAST feed page...")
    page = client.get_feed_page(last=True)
    items = page.get("items", [])
    
    if not items:
        logger.info("No items in last feed page.")
        return

    # Check the first few items
    for item in items[:1]:
        entry_id = item["id"]
        logger.info(f"Fetching details for entry: {entry_id}")
        
        details = client.get_feed_entry(entry_id)
        ad = details.get("ad_content", {})
        
        logger.info("--- POTENTIAL MISSING FIELDS ---")
        logger.info(f"Published: {ad.get('published')}")
        logger.info(f"Expires: {ad.get('expires')}")
        logger.info(f"Updated: {ad.get('updated')}")
        
        employer = ad.get("employer", {})
        logger.info(f"Employer keys: {employer.keys()}")
        logger.info(f"Employer: {json.dumps(employer, indent=2)}")
        
        logger.info(f"Source URL: {ad.get('sourceurl')}")
        logger.info(f"Job Title (styrk?): {ad.get('jobtitle')}")
        logger.info(f"Ad Title: {ad.get('title')}")

if __name__ == "__main__":
    debug_single_entry()
