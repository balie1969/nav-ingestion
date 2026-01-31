import os
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv

def inspect_db():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not set in .env")
        return

    try:
        engine = create_engine(db_url)
        inspector = inspect(engine)
        
        print("--- Schemas ---")
        for schema_name in inspector.get_schema_names():
            print(f"Schema: {schema_name}")
            print(f"Tables in {schema_name}:")
            for table_name in inspector.get_table_names(schema=schema_name):
                print(f"  - {table_name}")
                try:
                    columns = inspector.get_columns(table_name, schema=schema_name)
                    for column in columns:
                        print(f"    - {column['name']} ({column['type']})")
                except Exception as col_e:
                    print(f"    Error reading columns for {table_name}: {col_e}")

    except Exception as e:
        print(f"Error connecting to database: {e}")

if __name__ == "__main__":
    inspect_db()
