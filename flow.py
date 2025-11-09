from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.blocks.system import Secret
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_DICT = Secret.load("supabase-secret").get()

SUPABASE_URL = SUPABASE_DICT["SUPABASE_URL"]
SUPABASE_KEY = SUPABASE_DICT["SUPABASE_KEY"]

# Ensure environment variables are loaded
if not SUPABASE_URL:
    raise ValueError("SUPABASE_URL environment variable is not set")
if not SUPABASE_KEY:
    raise ValueError("SUPABASE_KEY environment variable is not set")

@task
def check_db():
    logger = get_run_logger()
    try:
        url = f"{SUPABASE_URL}/rest/v1/hotels"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()
        logger.info(f"DB check successful: {data}")
        print("Your DB is Active!")
        return data
    except requests.RequestException as e:
        logger.error(f"DB check failed: {e}")
        raise

@flow
def db_check():
    result = check_db()
    return result

if __name__ == "__main__":
    db_check()
