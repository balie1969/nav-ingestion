import os
import argparse
import logging
import time
from datetime import datetime
from dotenv import load_dotenv
from nav_client import NavClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Fast Forward to Date in Feed")
    parser.add_argument("target_date", type=str, help="YYYY-MM-DD to reach")
    args = parser.parse_args()
    
    load_dotenv()
    client = NavClient(os.getenv("NAV_API_TOKEN"))
    
    logger.info(f"Seeking feed page near {args.target_date}...")
    logger.info("Starting optimized sequential crawl from 2023...")
    
    current_id = None
    page_count = 0
    target_date_iso = f"{args.target_date}T00:00:00"
    
    # First fetch: Explicitly start at beginning
    page = client.get_feed_page(last=False)
    current_id = page.get("id")
    
    while True:
        try:
            # Subsequent fetches use ID
            if page_count > 0:
                 page = client.get_feed_page(page_id=current_id)
            
            items = page.get("items", [])
            next_id = page.get("next_id")
            
            if items:
                # Check date of LAST item (latest on page)
                last_item = items[-1]
                date_str = last_item.get("date_modified")
                
                # Check if we reached target
                if date_str and date_str >= target_date_iso:
                     logger.info(f"FOUND IT! Page {page.get('id')} contains date {date_str}")
                     print(f"ID: {page.get('id')}")
                     return
            
            if not next_id:
                logger.info("End of feed reached without finding target date (maybe it's in the future?)")
                break
                
            current_id = next_id
            page_count += 1
            if page_count % 500 == 0:
                short_date = items[-1].get("date_modified")[:10] if items else "???"
                logger.info(f"Scanned {page_count} pages... Current date: {short_date}")
                
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()
