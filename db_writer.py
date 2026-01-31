import logging
import json
from sqlalchemy import create_engine, text
from datetime import datetime

logger = logging.getLogger(__name__)

class DBWriter:
    def __init__(self, db_url):
        # Increase pool size to handle concurrent threads (default is 5)
        # We use 10 threads, so 20 is safe margin.
        self.engine = create_engine(db_url, pool_size=20, max_overflow=10)

    def save_job(self, job_data):
        """
        Saves a job and its related data to the database using a transaction.
        """
        uuid = job_data.get("uuid")
        if not uuid:
            logger.warning("Job data missing UUID, skipping save.")
            return

        with self.engine.begin() as conn:
            # 1. Upsert nav_jobs
            self._upsert_nav_job(conn, job_data)

            # Extract ad_content for related data
            ad_content = job_data.get("ad_content", {})

            # 2. Update related tables (delete old, insert new)
            # Locations
            conn.execute(text("DELETE FROM nav_job_locations WHERE nav_uuid = :uuid"), {"uuid": uuid})
            self._insert_locations(conn, uuid, ad_content.get("workLocations", []))

            # Contacts
            conn.execute(text("DELETE FROM nav_job_contacts WHERE nav_uuid = :uuid"), {"uuid": uuid})
            self._insert_contacts(conn, uuid, ad_content.get("contactList", []))

            # Categories
            conn.execute(text("DELETE FROM nav_job_categories WHERE nav_uuid = :uuid"), {"uuid": uuid})
            self._insert_categories(conn, uuid, ad_content.get("categoryList", []))

            # Occupations
            conn.execute(text("DELETE FROM nav_job_occupations WHERE nav_uuid = :uuid"), {"uuid": uuid})
            self._insert_occupations(conn, uuid, ad_content.get("occupationCategories", []))

    def _upsert_nav_job(self, conn, job):
        ad_content = job.get("ad_content", {})
        # Flatten structure a bit for the main table
        # Note: The API structure might vary slightly, ad_content usually holds most fields if we fetched 'feedentry'
        # But 'feedentry' structure from API docs shows fields directly on root or inside ad_content?
        # Let's inspect what we passed in. The client yields 'feedentry' response content.
        # The schema says 'FeedEntryContent' has 'ad_content' (FeedAd).
        # Most fields are inside 'ad_content'.
        
        # If the job object passed here is the 'FeedAd' object directly or the 'FeedEntryContent'?
        # In main.py we saw: content = job.get("ad_content", {})
        # So 'job' here is likely the whole response from /feedentry/{id} which has 'uuid', 'status', 'ad_content'.
        
        uuid = job.get("uuid")
        status = job.get("status")
        nav_updated_at = job.get("sistEndret") # This is often at root of FeedEntryContent
        
        # Details are in ad_content
        ad = job.get("ad_content") or {}
        
        # Helper to safely parse dates
        def parse_dt(dt_str):
            if not dt_str: return None
            try:
                return datetime.fromisoformat(dt_str)
            except ValueError:
                return None

        # Helper for employer name and details
        employer = ad.get("employer", {})
        if isinstance(employer, dict):
            employer_name = employer.get("name")
            employer_orgnr = employer.get("orgnr")
            employer_desc = employer.get("description")
            employer_homepage = employer.get("homepage")
        else:
            employer_name = str(employer)
            employer_orgnr = None
            employer_desc = None
            employer_homepage = None
            
        # Work locations (first one for main table)
        locations = ad.get("workLocations", [])
        loc = locations[0] if locations else {}
        
        params = {
            "nav_uuid": uuid,
            "job_url": ad.get("link"),
            "job_title": ad.get("title"),
            "company": employer_name,
            "job_text_html": ad.get("description"), # HTML content
            "kommune": loc.get("municipal"),
            "fylke": loc.get("county"),
            "omfang": ad.get("extent"),
            "ansettelsesform": ad.get("engagementtype"),
            "sektor": ad.get("sector"),
            "frist_dato": ad.get("applicationDue"), # Might be string, keep as string or try parse? Schema says TIMESTAMP.
            # wait, frist_dato in DB is TIMESTAMP. API returns string. 
            # Often 'applicationDue' is "Snarest" or a date. We need check.
            # If it's not a valid date, we might store null or handle it.
            # DB schema demands TIMESTAMP? Let's assume we try parse or NULL.
            "yrkeskode": None, # occupationCategories usually used instead
            "kilde": ad.get("source"),
            "nav_updated_at": parse_dt(nav_updated_at) or parse_dt(ad.get("updated")),
            "status": status,
            "expires": parse_dt(ad.get("expires")),
            "engagementtype": ad.get("engagementtype"),
            "extent": ad.get("extent"),
            "starttime": ad.get("starttime"),
            "positioncount": ad.get("positioncount"),
            "sector": ad.get("sector"),
            "applicationurl": ad.get("applicationUrl"),
            "created_at": datetime.now(), # We set this on insert
            # New fields:
            "published": parse_dt(ad.get("published")),
            "job_title_official": ad.get("jobtitle"),
            "employer_orgnr": employer_orgnr,
            "employer_description": employer_desc,
            "employer_homepage": employer_homepage
        }

        # Handling date parsing for frist_dato specifically
        # ad['applicationDue'] can be "Snarest", "30.01.2025", etc.
        # If it's not ISO format, we likely can't store in TIMESTAMP column easily without robust parsing.
        # For now, let's try to parse if ISO, else NULL.
        # If the user wants specific handling for "Snarest", we'll need a text column or custom logic.
        # Given DB has TIMESTAMP, we'll try strict parse.
        try:
            params["frist_dato"] = datetime.fromisoformat(ad.get("applicationDue"))
        except (ValueError, TypeError):
             params["frist_dato"] = None

        sql = text("""
            INSERT INTO nav_jobs (
                nav_uuid, job_url, job_title, company, job_text_html, kommune, fylke, 
                omfang, ansettelsesform, sektor, frist_dato, kilde, 
                nav_updated_at, status, expires, engagementtype, extent, 
                starttime, positioncount, sector, applicationurl, created_at,
                published, job_title_official, employer_orgnr, employer_description, employer_homepage
            ) VALUES (
                :nav_uuid, :job_url, :job_title, :company, :job_text_html, :kommune, :fylke, 
                :omfang, :ansettelsesform, :sektor, :frist_dato, :kilde, 
                :nav_updated_at, :status, :expires, :engagementtype, :extent, 
                :starttime, :positioncount, :sector, :applicationurl, :created_at,
                :published, :job_title_official, :employer_orgnr, :employer_description, :employer_homepage
            )
            ON CONFLICT (nav_uuid) DO UPDATE SET
                job_url = EXCLUDED.job_url,
                job_title = EXCLUDED.job_title,
                company = EXCLUDED.company,
                job_text_html = EXCLUDED.job_text_html,
                kommune = EXCLUDED.kommune,
                fylke = EXCLUDED.fylke,
                omfang = EXCLUDED.omfang,
                ansettelsesform = EXCLUDED.ansettelsesform,
                sektor = EXCLUDED.sektor,
                frist_dato = EXCLUDED.frist_dato,
                kilde = EXCLUDED.kilde,
                nav_updated_at = EXCLUDED.nav_updated_at,
                status = EXCLUDED.status,
                expires = EXCLUDED.expires,
                engagementtype = EXCLUDED.engagementtype,
                extent = EXCLUDED.extent,
                starttime = EXCLUDED.starttime,
                positioncount = EXCLUDED.positioncount,
                sector = EXCLUDED.sector,
                applicationurl = EXCLUDED.applicationurl,
                published = EXCLUDED.published,
                job_title_official = EXCLUDED.job_title_official,
                employer_orgnr = EXCLUDED.employer_orgnr,
                employer_description = EXCLUDED.employer_description,
                employer_homepage = EXCLUDED.employer_homepage;
        """)
        conn.execute(sql, params)

    def _insert_locations(self, conn, uuid, locations):
        if not locations: return
        data = []
        seen = set()
        
        for loc in locations:
            # Create a tuple of relevant fields to check for duplicates
            # (address, city, postalCode, country) usually define a unique location for a job
            key = (loc.get("address"), loc.get("city"), loc.get("postalCode"), loc.get("country"), loc.get("municipal"), loc.get("county"))
            if key not in seen:
                seen.add(key)
                data.append({
                    "nav_uuid": uuid,
                    "address": loc.get("address"),
                    "city": loc.get("city"),
                    "postal_code": loc.get("postalCode"),
                    "county": loc.get("county"),
                    "municipal": loc.get("municipal"),
                    "country": loc.get("country")
                })
                
        if data:
            conn.execute(text("""
                INSERT INTO nav_job_locations (nav_uuid, address, city, postal_code, county, municipal, country)
                VALUES (:nav_uuid, :address, :city, :postal_code, :county, :municipal, :country)
            """), data)

    def _insert_contacts(self, conn, uuid, contacts):
        if not contacts: return
        data = []
        for c in contacts:
            data.append({
                "nav_uuid": uuid,
                "name": c.get("name"),
                "email": c.get("email"),
                "phone": c.get("phone"),
                "role": c.get("role"),
                "title": c.get("title")
            })
        if data:
            conn.execute(text("""
                INSERT INTO nav_job_contacts (nav_uuid, name, email, phone, role, title)
                VALUES (:nav_uuid, :name, :email, :phone, :role, :title)
            """), data)

    def _insert_categories(self, conn, uuid, categories):
        if not categories: return
        data = []
        seen = set()
        for c in categories:
            # Create a unique key for deduplication
            key = (c.get("categoryType"), c.get("code"), c.get("name"))
            if key not in seen:
                seen.add(key)
                data.append({
                    "nav_uuid": uuid,
                    "category_type": c.get("categoryType"),
                    "code": c.get("code"),
                    "name": c.get("name")
                })
        if data:
            conn.execute(text("""
                INSERT INTO nav_job_categories (nav_uuid, category_type, code, name)
                VALUES (:nav_uuid, :category_type, :code, :name)
            """), data)

    def _insert_occupations(self, conn, uuid, occupations):
        if not occupations: return
        data = []
        seen = set()
        for o in occupations:
            l1 = o.get("level1")
            l2 = o.get("level2")
            key = (l1, l2)
            if key not in seen:
                seen.add(key)
                data.append({
                    "nav_uuid": uuid,
                    "level1": l1,
                    "level2": l2
                })
        if data:
            conn.execute(text("""
                INSERT INTO nav_job_occupations (nav_uuid, level1, level2)
                VALUES (:nav_uuid, :level1, :level2)
            """), data)

    def ensure_feed_state_schema(self):
        """
        Ensures that nav_feed_state has the required columns.
        """
        required_columns = {
            "title": "TEXT",
            "home_page_url": "TEXT",
            "feed_url": "TEXT",
            "description": "TEXT"
        }
        self._ensure_columns("nav_feed_state", required_columns)

    def ensure_job_schema_enhancements(self):
        """
        Ensures that nav_jobs has the new enriched columns.
        """
        required_columns = {
            "published": "TIMESTAMP",
            "job_title_official": "TEXT",
            "employer_orgnr": "VARCHAR(50)",
            "employer_description": "TEXT",
            "employer_homepage": "TEXT"
        }
        self._ensure_columns("nav_jobs", required_columns)

    def _ensure_columns(self, table_name, columns):
        with self.engine.connect() as conn:
            # Check existing columns
            result = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"))
            existing = {row[0] for row in result}
            
            for col, dtype in columns.items():
                if col not in existing:
                    logger.info(f"Adding column {col} to {table_name}")
                    conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {col} {dtype}"))
                    conn.commit()

    def get_last_feed_state(self):
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT next_url, last_job_date FROM nav_feed_state ORDER BY id DESC LIMIT 1"))
            row = result.fetchone()
            if row:
                return {"next_url": row[0], "last_job_date": row[1]}
        return None

    def update_feed_state(self, next_url, last_job_date=None, metadata=None):
        """
        Updates the feed state and optionally the feed metadata.
        """
        metadata = metadata or {}
        
        with self.engine.begin() as conn:
            # We preserve only one state row usually, or append?
            # Let's just update the single row if it exists, or insert.
            # Schema has 'id' PK.
            # Simple approach: Delete all and insert new.
            conn.execute(text("DELETE FROM nav_feed_state"))
            
            params = {
                "next_url": next_url,
                "last_job_date": last_job_date,
                "updated_at": datetime.now(),
                "title": metadata.get("title"),
                "home_page_url": metadata.get("home_page_url"),
                "feed_url": metadata.get("feed_url"),
                "description": metadata.get("description")
            }
            
            conn.execute(text("""
                INSERT INTO nav_feed_state (
                    next_url, last_job_date, updated_at, 
                    title, home_page_url, feed_url, description
                ) VALUES (
                    :next_url, :last_job_date, :updated_at, 
                    :title, :home_page_url, :feed_url, :description
                )
            """), params)
