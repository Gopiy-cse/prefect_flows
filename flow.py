from prefect import flow, task
from prefect.logging import get_run_logger
import requests
import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

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
