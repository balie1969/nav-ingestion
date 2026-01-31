import os
import logging
import argparse
from datetime import datetime
from dotenv import load_dotenv
from nav_client import NavClient
from db_writer import DBWriter

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Sync NAV Job Feed to Database")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of jobs to save in this run")
    parser.add_argument("--reset", action="store_true", help="Clear feed state and start over")
    parser.add_argument("--start-from-beginning", action="store_true", help="If resetting, start from the very first page (2023) instead of latest")
    parser.add_argument("--start-date", type=str, help="Filter jobs older than this date (YYYY-MM-DD)")
    parser.add_argument("--start-page-id", type=str, help="Manually set the page ID to start/resume from (overrides DB state)")
    return parser.parse_args()

def main():
    args = parse_args()
    load_dotenv()
    
    api_token = os.getenv("NAV_API_TOKEN")
    db_url = os.getenv("DATABASE_URL")

    if not api_token:
        logger.error("NAV_API_TOKEN environment variable is not set.")
        return
    if not db_url:
        logger.error("DATABASE_URL environment variable is not set.")
        return

    client = NavClient(api_token=api_token)
    db = DBWriter(db_url)

    db.ensure_feed_state_schema()
    db.ensure_job_schema_enhancements()
    db.ensure_materialized_views()
    
    # Parse start_date if provided
    filter_date = None
    if args.start_date:
        try:
            filter_date = datetime.fromisoformat(args.start_date).replace(tzinfo=None) # Naive for comparison if needed, or keeping TZ
            # Actually, robust comparison matches TZ. Let's assume input is YYYY-MM-DD
            filter_date = datetime.strptime(args.start_date, "%Y-%m-%d")
            logger.info(f"Filtering jobs older than {filter_date}")
        except ValueError:
            logger.error("Invalid date format. Use YYYY-MM-DD")
            return

    logger.info("Starting job sync...")
    if args.limit:
        logger.info(f"Batch limit set to: {args.limit}")

    # Handle Reset
    if args.reset:
        logger.warning("Resetting feed state!")
        with db.engine.begin() as conn:
             from sqlalchemy import text
             conn.execute(text("DELETE FROM nav_feed_state"))
        logger.info("Feed state cleared.")

    # 1. Get last state
    state = db.get_last_feed_state()
    start_page_id = None
    
    try:
        # Determine start point
        if args.start_page_id:
             logger.info(f"Manual start page ID provided: {args.start_page_id}")
             start_page_id = args.start_page_id
             page_iterator = client.fetch_feed_pages(start_page_id=start_page_id)
        elif state and state.get("next_url"):
            # Resume from state
            next_url = state.get("next_url")
            start_page_id = None
            if "/feed/" in next_url:
                start_page_id = next_url.split("/feed/")[-1]
            else:
                 start_page_id = next_url
            
            logger.info(f"Resuming from page ID: {start_page_id}")
            page_iterator = client.fetch_feed_pages(start_page_id=start_page_id)
        else:
            # First run or Reset
            if args.start_from_beginning and args.reset:
                 logger.info("Starting from the BEGINNING of the feed (2023)...")
                 page_iterator = client.fetch_feed_pages(last=False)
            else:
                 logger.info("No state found (or defaulting to new). Starting from LAST page (newest jobs).")
                 page_iterator = client.fetch_feed_pages(last=True)

        fetched_count = 0
        skipped_count = 0
        
        # Init ThreadPool
        import concurrent.futures
    
        # We can fetch concurrently. Session is thread-safe.
        # Max workers = 10 to be safe with rate limits? 
        MAX_WORKERS = 10
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for page in page_iterator:
                items = page.get("items", [])
                logger.info(f"Processing page with {len(items)} items using {MAX_WORKERS} threads...")
                
                # Pre-filter items to save fetch calls
                items_to_fetch = []
                seen_ids_in_batch = set()
                
                for item in items:
                    item_id = item.get("id")
                    if item_id in seen_ids_in_batch:
                        continue
                    seen_ids_in_batch.add(item_id)
                    
                    # OPTIMIZATION: Check date from feed summary FIRST
                    if filter_date:
                        summary_date_str = item.get("date_modified")
                        if summary_date_str:
                             try:
                                 summary_date = datetime.fromisoformat(summary_date_str).replace(tzinfo=None)
                                 if summary_date < filter_date:
                                     skipped_count += 1
                                     continue
                             except ValueError:
                                 pass
                    items_to_fetch.append(item)

                if not items_to_fetch:
                    logger.info("All items on this page skipped by date filter.")
                    # Update state and continue
                    if page.get("next_url"):
                         db.update_feed_state(next_url=page.get("next_url"))
                    continue

                # Fetch details in parallel
                # We map (lambda ID: client.get_feed_entry(ID))
                # But we ideally usually need the raw item too for error logging, but client uses ID.
                # Let's just pass ID.
                
                logger.info(f"Fetching details for {len(items_to_fetch)} items...")
                
                # Map returns iterator.
                fetch_ids = [i["id"] for i in items_to_fetch]
                
                future_to_item = {executor.submit(client.get_feed_entry, item["id"]): item for item in items_to_fetch}
                
                page_fetched_count = 0
                
                for future in concurrent.futures.as_completed(future_to_item):
                    item_ref = future_to_item[future]
                    try:
                        details = future.result()
                        
                        if not details: 
                            skipped_count += 1
                            continue

                        # Check for ad content and active status
                        status = details.get("status")
                        
                        # 1. Filter Inactive/Empty
                        if status == "INACTIVE" or not details.get("ad_content"):
                             skipped_count += 1
                             continue
                        
                        # 2. Filter by Date (detailed check)
                        if filter_date:
                            ad = details.get("ad_content", {})
                            date_str = ad.get("published") or ad.get("updated")
                            if date_str:
                                try:
                                    job_dt = datetime.fromisoformat(date_str).replace(tzinfo=None)
                                    if job_dt < filter_date:
                                        skipped_count += 1
                                        continue
                                except:
                                    pass

                        db.save_job(details)
                        fetched_count += 1
                        page_fetched_count += 1
                        
                        if fetched_count % 50 == 0:
                            logger.info(f"Saved {fetched_count} valid jobs...")
                            
                        # Limit check
                        if args.limit and fetched_count >= args.limit:
                            # We must break inner loop and flag to break outer
                            break

                    except Exception as e:
                        logger.error(f"Error processing item {item_ref.get('id')}: {e}")

                # Limit check break outer
                if args.limit and fetched_count >= args.limit:
                     logger.info(f"Reached limit of {args.limit} saved jobs.")
                     # Save state!
                     current_id = page.get("id")
                     if current_id:
                          resume_url = f"/api/v1/feed/{current_id}"
                          db.update_feed_state(
                             next_url=resume_url,
                             metadata={
                                 "title": page.get("title"),
                                 "description": page.get("description")
                             }
                          )
                          logger.info(f"Limit reached. State saved to resume from current page: {resume_url}")
                     break

                # Update state AFTER processing page
                next_url = page.get("next_url")
                
                # End of feed check
                if not next_url:
                    current_id = page.get("id")
                    if current_id:
                         next_url = f"/api/v1/feed/{current_id}"
                         logger.info(f"End of feed reached. Saving current page {current_id} as resume point.")

                if next_url:
                    db.update_feed_state(
                        next_url=next_url,
                        metadata={
                            "title": page.get("title"),
                            "description": page.get("description")
                        }
                    )
                    logger.info(f"State updated. Next URL: {next_url}")
            
        logger.info(f"Sync complete. Saved {fetched_count} jobs. Skipped {skipped_count} jobs.")
        
        # Refresh search views for frontend
        db.refresh_materialized_views()

        # Update system parameter for last run
        db.update_system_parameter("NAV_Last_Update", datetime.now())
    
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
