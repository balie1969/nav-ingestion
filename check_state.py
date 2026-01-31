import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def check_state():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM nav_feed_state"))
        for row in result:
            print(row._mapping)

if __name__ == "__main__":
    check_state()
