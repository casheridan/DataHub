import os
import json
import requests
import logging
import subprocess
from datetime import datetime

# --- Configuration ---
LOCAL_API_URL = "http://127.0.0.1:8000/analytics"

# IMPORTANT: Set these as environment variables for security
GITHUB_USERNAME = os.getenv("GITHUB_USERNAME")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN") # Your Personal Access Token
GITHUB_REPO = os.getenv("GITHUB_REPO") # e.g., "my-username/my-vercel-site"

# Path to your cloned git repository folder
PROJECT_PATH = "C:/AnalyticsScript"
OUTPUT_FILENAME = "analytics_data.json"
OUTPUT_PATH = os.path.join(PROJECT_PATH, OUTPUT_FILENAME)

logging.basicConfig(level="INFO", format="%(asctime)s | %(levelname)s | %(message)s")

def run_command(command, working_dir):
    """Runs a shell command in a specified directory."""
    try:
        result = subprocess.run(command, cwd=working_dir, check=True, capture_output=True, text=True)
        logging.info(f"Command successful: {' '.join(command)}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running command: {' '.join(command)}\n{e.stderr}")
        return False

def git_sync_and_push():
    """Pulls latest changes, adds, commits, and pushes the data file."""
    commit_message = f"Data update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    remote_url = f"https://{GITHUB_USERNAME}:{GITHUB_TOKEN}@github.com/{GITHUB_REPO}.git"

    # Always pull first to avoid conflicts
    if not run_command(["git", "pull"], PROJECT_PATH):
        logging.error("Failed to pull latest changes. Aborting push.")
        return False

    # Check if the data file has actually changed
    status_check = subprocess.run(["git", "status", "--porcelain"], cwd=PROJECT_PATH, capture_output=True, text=True)
    if OUTPUT_FILENAME not in status_check.stdout:
        logging.info("No changes to analytics_data.json. Nothing to commit.")
        return True

    if not run_command(["git", "add", OUTPUT_FILENAME], PROJECT_PATH): return False
    if not run_command(["git", "commit", "-m", commit_message], PROJECT_PATH): return False
    if not run_command(["git", "push", remote_url, "main"], PROJECT_PATH): return False
    
    logging.info("Successfully pushed data update to GitHub.")
    return True

# (Keep the fetch_analytics_data and write_data_to_json functions as before)

def fetch_analytics_data():
    """Fetches aggregated data from the local FastAPI analytics endpoint."""
    logging.info(f"Fetching data from {LOCAL_API_URL}")
    try:
        response = requests.get(LOCAL_API_URL, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data: {e}")
        return None

def write_data_to_json(data):
    """Writes the provided data to the JSON file inside the git repo."""
    if data is None: return
    try:
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f)
        logging.info(f"Successfully wrote data to {OUTPUT_PATH}")
    except IOError as e:
        logging.error(f"Failed to write to file: {e}")

def main():
    """Main execution function."""
    if not all([GITHUB_USERNAME, GITHUB_TOKEN, GITHUB_REPO, PROJECT_PATH]):
        logging.error("Missing required configuration. Check environment variables and PROJECT_PATH.")
        return
        
    data = fetch_analytics_data()
    write_data_to_json(data)
    git_sync_and_push()
    logging.info("Push script finished.")

if __name__ == "__main__":
    main()