from settings import settings
import sitemap
import pandas as pd
import requests
from curl_cffi import requests as cureq
from pydantic import BaseModel
from rich import print
from datetime import datetime, timedelta
from requests.exceptions import RequestException
import random
import time
from tqdm import tqdm

#headers = {"User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0"}

# Sample user agents
#user_agents = [
#    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
#    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
#    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0"
#]

def get_region_IDs(headers, user_agents):

    # Store region codes
    region_codes = []

    # Loop through IDs
    for ID in range(100):
        search_data = {}
        # Format the URL
        URL = f'https://platform.realestate.co.nz/search/v1/properties?filter%5Bregion%5D%5B%5D={ID}&filter%5BsaleDateMin%5D={datetime.today().date().strftime("%Y-%m-%d")}'

        # Use a random user-agent for each request
        headers["User-Agent"] = random.choice(user_agents)

        try:
            # Send the request
            resp = requests.get(URL, headers=headers)

            # Handle non-200 responses
            if resp.status_code != 200:
                print(f"Skipping ID {ID} due to HTTP {resp.status_code}")
                time.sleep(random.uniform(0.8, 2))  # Random sleep between 0.8-2 seconds
                continue

            # Parse JSON response
            data = resp.json()

            # Extract region information
            if data["meta"]["search"]["location"]["regions"]:
                region_info = data["meta"]["search"]["location"]["regions"][0]
                search_data["regionID"] = region_info["id"]
                search_data["region"] = region_info["attributes"]["title"]
                search_data["region_slug"] = region_info["attributes"]["slug"]

                print(f'Obtained ID: {ID}: {search_data["region"]}')
                
                # Append region data to the list
                region_codes.append(search_data)


        except Exception as e:
            print(f"Error processing ID {ID}: {e}")

        # Optional: Pause between requests
        time.sleep(random.uniform(0.8, 2))  # Random sleep between 0.8-2 seconds

    # Convert region codes to a DataFrame
    region_codes_df = pd.DataFrame(region_codes)

    #region_codes_df.to_csv(settings.region_table, index=False)

    return region_codes_df

#region_codes_df = get_region_IDs(headers, user_agents)