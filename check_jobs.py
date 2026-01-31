import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def check_jobs():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    
    with engine.connect() as conn:
        print("--- Checking nav_jobs ---")
        # Select ones with title
        result = conn.execute(text("SELECT nav_uuid, job_title, published, job_title_official, employer_orgnr, employer_homepage FROM nav_jobs WHERE job_title IS NOT NULL LIMIT 5"))
        rows = result.fetchall()
        if not rows:
            print("No jobs with title found.")
        
        for row in rows:
            uuid = row[0]
            print(f"UUID: {uuid}")
            print(f"  Title: {row[1]}")
            print(f"  Published: {row[2]}")
            print(f"  Official Title: {row[3]}")
            print(f"  OrgNr: {row[4]}")
            print(f"  Homepage: {row[5]}")
            
            # Check related
            locs = conn.execute(text("SELECT COUNT(*) FROM nav_job_locations WHERE nav_uuid = :uuid"), {"uuid": uuid}).scalar()
            # contacts = conn.execute(text("SELECT COUNT(*) FROM nav_job_contacts WHERE nav_uuid = :uuid"), {"uuid": uuid}).scalar()
            # cats = conn.execute(text("SELECT COUNT(*) FROM nav_job_categories WHERE nav_uuid = :uuid"), {"uuid": uuid}).scalar()
            
            # print(f"  -> Locations: {locs}, Contacts: {contacts}, Categories: {cats}")

if __name__ == "__main__":
    check_jobs()
