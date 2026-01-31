# NAV Job Feed Fetcher - Walkthrough

This guide explains how to use the Python script to fetch job vacancies from NAV.

## Prerequisites

1.  **Python 3**: Ensure Python is installed.
2.  **NAV API Token**: You need a Bearer token from NAV/Arbeidsplassen.

## Setup

1.  **Navigate to the project directory**:
    ```bash
    cd /Users/bjornandre.lie/Documents/dev/jobsai-backend
    ```

2.  **Activate the virtual environment**:
    ```bash
    source .venv/bin/activate
    ```
    (Dependencies are already installed).

3.  **Configure the API Token**:
    - Rename `.env.example` to `.env`:
      ```bash
      mv .env.example .env
      ```
    - Open `.env` and paste your token:
      ```
      NAV_API_TOKEN=your_actual_token_here
      ```

## Configuring the Database

1.  **Environment Variables**:
    Ensure your `.env` file has the `DATABASE_URL` set:
    ```
    NAV_API_TOKEN=your_token_here
    DATABASE_URL="postgresql://user:password@localhost:5432/jobsai"
    ```

2.  **Schema**:
    The script expects the `nav_jobs` and related tables to exist in the `public` schema.

## Run the Sync

Execute the main script using the python binary in the virtual environment:
```bash
./.venv/bin/python3 main.py
```

### Advanced Usage (CLI Parameters)

You can control the synchronization process with command-line arguments:

| Argument | Description | Example |
| :--- | :--- | :--- |
| `--limit <N>` | Process max N valid jobs then stop. Good for testing. | `./.venv/bin/python3 main.py --limit 10` |
| `--reset` | Clear feed state history. **Default:** Starts from LAST page (newest). | `./.venv/bin/python3 main.py --reset` |
| `--start-from-beginning` | Use with `--reset` to start from the first page (2023). | `./.venv/bin/python3 main.py --reset --start-from-beginning` |
| `--start-date <YYYY-MM-DD>` | Skip jobs updated before date. | `./.venv/bin/python3 main.py --start-date 2026-01-01` |

**Examples:**

1. **Full Refresh from 2025 (Catch-up):**
   Use this to clear history and fetch all relevant jobs from a specific date.
   ```bash
   ./.venv/bin/python3 main.py --reset --start-from-beginning --start-date 2025-01-01
   ```

2. **Quick Test (Fetch 5 jobs):**
   Useful for verifying the setup without waiting.
   ```bash
   ./.venv/bin/python3 main.py --limit 5
   ```

3. **Reset to Latest:**
   Start fresh but only care about new jobs from now on.
   ```bash
   ./.venv/bin/python3 main.py --reset
   ```

### Combining Parameters
Parameters are modular and can be combined freely:
- `--limit` can be used with any mode to stop early.
- `--start-date` works best with `--start-from-beginning` but can also be used alone if you just want to filter the current batch.
- `--reset` is required if you want to ignore the stored state in the database.

## What to Expect

The script will:
1.  Connect to the NAV Job Feed API and your PostgreSQL database.
2.  Fetch jobs from the feed (either resuming or starting fresh).
3.  Upsert jobs into the `nav_jobs` table and update related tables (locations, contacts, etc.).
4.  Log the progress to the console (including "Fast-skipped" counts for filtered jobs).

## Feed Metadata

The script automatically fetches and stores feed metadata (like title, description, URLs) in the `nav_feed_state` table.

## Performance Optimization
To speed up the data sync, the script uses **concurrent fetching**:
- The script uses a `ThreadPoolExecutor` with 10 worker threads.
- Instead of fetching job details one by one (synchronous), it fetches 10 jobs at a time in parallel.
- This significantly increases throughput, limited mostly by network latency and API rate limits.
- **Deduplication**: The script automatically filters out duplicate job IDs within the same batch to prevent race conditions.

## Troubleshooting
### "value too long for type character varying(20)"
- If getting this error, likely the database schema has a short column limit.
- Fix: Determine the column (e.g. `kilde` or `employer_orgnr`) and change type to `TEXT` or `VARCHAR(255)`.
- The current codebase uses `TEXT` for most variable length fields to avoid this.

### "duplicate key value violates unique constraint"
- Occurs if the API returns the same job UUID multiple times.
- **Fix**: The script has logic to skip duplicates within the same processing batch.
- **Fix**: The database writer uses `ON CONFLICT DO UPDATE` for the main job table, ensuring updates are handled gracefully.
