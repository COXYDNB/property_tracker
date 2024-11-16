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

BASE_URL = settings.properties_url

#Remove this later on
sitemap_df = sitemap.get_sitemap()

# get existing tables
Property = pd.read_csv(settings.property_table)
Valuation = pd.read_csv(settings.valuation_table)

# Get the property IDs that are 'sold' from sitemap_df
sitemap_sold_property_IDs = sitemap_df[sitemap_df['listing_type'] == "sold"]['ID']

# Remove unnecessary filters later
sitemap_sold_property_IDs = sitemap_sold_property_IDs[sitemap_df['property_type'] == "residential"]
sitemap_sold_property_IDs = sitemap_sold_property_IDs[sitemap_df['region'] == "central-otago-lakes-district"]

# Convert sitemap_sold_property_IDs into a DataFrame with the column 'id' for merging
sitemap_sold_property_IDs_df = sitemap_sold_property_IDs.to_frame(name='id')

# Perform an outer join
Property_IDs_Dates = pd.merge(Property[['id', 'last_update']], sitemap_sold_property_IDs_df, on='id', how='outer')

# Replace null (NaT) values in 'last_update' with the default date '1900-01-01'
Property_IDs_Dates['last_update'] = Property_IDs_Dates['last_update'].fillna(pd.to_datetime('1900-01-01'))

# Convert 'last_update' to datetime for comparison
Property_IDs_Dates['last_update'] = pd.to_datetime(Property_IDs_Dates['last_update'])

# Get the date of one week ago from today
one_month_ago = datetime.today() - timedelta(weeks=4)

x1 = Property_IDs_Dates.shape

# Filter properties that have not been updated in the last week
Properties_not_updated_recently = Property_IDs_Dates[Property_IDs_Dates['last_update'] < one_month_ago]

x2 = Properties_not_updated_recently.shape

print(f'{x1[0]} properties to begin with...')
print(f'{x2[0]} properties to update...')

property_IDs_to_update = Properties_not_updated_recently['id']

headers = {"User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0"}

# Sample user agents
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0"
]

def get_updated_data(property_IDs_to_update, BASE_URL, headers):
    new_property_data = []  # List to store property data DataFrames
    new_valuation_data = []  # List to store valuation data DataFrames

    retries = 3  # Number of retries for failed requests
    backoff = 2  # Exponential backoff factor

    # Wrap the property ID list with tqdm for a progress bar
    for ID in tqdm(property_IDs_to_update, desc="Processing Properties", unit="property"):
        for attempt in range(retries):
            try:
                # Use a random user-agent for each request to avoid detection
                headers['User-Agent'] = random.choice(user_agents)

                # Send the request
                resp = requests.get(f'{BASE_URL}{ID}', headers=headers)

                # Check for response status
                if resp.status_code == 200:
                    data = resp.json()
                    break  # Exit the retry loop if the request was successful
                elif resp.status_code == 429:
                    print(f"Rate limit exceeded for {ID}. Retrying in {backoff ** attempt} seconds...")
                    time.sleep(backoff ** attempt)  # Exponential backoff
                else:
                    print(f"Failed to fetch data for {ID}. HTTP Status: {resp.status_code}")
                    break  # Exit loop on other HTTP errors

            except RequestException as e:
                print(f"Request error for {ID}: {e}")
                time.sleep(2)  # Short pause before retrying

        else:  # If all retries fail, log and continue to next property
            print(f"Failed to retrieve data for {ID} after {retries} attempts.")
            continue

        # Check if 'data['data']' is a dictionary (not a list)
        if not isinstance(data['data'], dict):  # If it's not a dictionary, log and skip to next
            print(f"Data for ID {ID} is not in expected format. Skipping...")
            continue  # Skip to the next property ID

        # Property data
        try:
            prop_data = {}
            prop_data['id'] = ID
            prop_data['type'] = data['data']['type']  # Directly accessing 'type' from 'data'
            prop_data['bedrooms'] = data['data']['attributes'].get('bedrooms-total-count', None)
            prop_data['bathrooms'] = data['data']['attributes'].get('bathrooms-total-count', None)
            prop_data['floor_area'] = data['data']['attributes'].get('floor-area', None)
            prop_data['land_area'] = data['data']['attributes'].get('land-area', None)
            prop_data['land_area_unit'] = data['data']['attributes'].get('land-area-unit', None)
            prop_data['floor_area_unit'] = data['data']['attributes'].get('floor-area-unit', None)
            prop_data['bathroom_ensuite_count'] = data['data']['attributes'].get('bathroom-ensuite-count', None)
            prop_data['bathroom_wc_count'] = data['data']['attributes'].get('bathroom-wc-count', None)
            prop_data['parking_covered_count'] = data['data']['attributes'].get('parking-covered-count', None)
            prop_data['has_swimming_pool'] = data['data']['attributes'].get('has-swimming-pool', None)
            prop_data['storey_count'] = data['data']['attributes'].get('storey-count', None)
            prop_data['parking_other_count'] = data['data']['attributes'].get('parking-other-count', None)

            # Address
            address = data['data']['attributes'].get('address', {})
            prop_data['unit_number'] = address.get('unit-number', None)
            prop_data['street_number'] = address.get('street-number', None)
            prop_data['street_number_suffix'] = address.get('street-number-suffix', None)
            prop_data['street_name'] = address.get('street-name', None)
            prop_data['suburb'] = address.get('suburb', None)
            prop_data['district'] = address.get('district', None)
            prop_data['region'] = address.get('region', None)
            prop_data['postcode'] = address.get('postcode', None)
            prop_data['latitude'] = address.get('latitude', None)
            prop_data['longitude'] = address.get('longitude', None)
            # Check if 'latest-sale-listing' exists and is not an empty list
            latest_sale_listing = data['data']['attributes'].get('latest-sale-listing', [])
            if latest_sale_listing:
                prop_data['latest_sale_listing_date'] = datetime.strptime(latest_sale_listing['listing-published-date'], '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d')
            else:
                prop_data['latest_sale_listing_date'] = None  # Set to None or a default value if no data

            prop_data['latest_sale_date'] = data['data']['attributes']['latest-sale-sort-date']
            prop_data['last_update'] = datetime.today().strftime('%Y-%m-%d')

            # Append property data as DataFrame to the list
            new_property_data.append(pd.DataFrame([prop_data]))  # Append a single-row DataFrame

            # Valuation data (Ensure we handle null 'estimated-values')
            if data['data']['attributes'].get('estimated-values', None):
                for valuation in data['data']['attributes']['estimated-values']:
                    val_data = {}
                    val_data['id'] = ID
                    val_data['estimated_date'] = valuation.get('estimated-date', None)
                    val_data['value_low'] = valuation.get('value-low', None)
                    val_data['value_mid'] = valuation.get('value-mid', None)
                    val_data['value_high'] = valuation.get('value-high', None)
                    val_data['confidence_rating'] = valuation.get('confidence-rating', None)

                    # Append valuation data as DataFrame to the list
                    new_valuation_data.append(pd.DataFrame([val_data]))  # Append a single-row DataFrame

        except Exception as e:
            print(f"Error processing data for ID {ID}: {e}")
            print(f"Exiting function due to error with ID {ID}.")
            return  # Exit the function

        # Optional: Pause between requests to reduce server load
        time.sleep(random.uniform(1, 3))  # Random sleep between 1-3 seconds

    # Concatenate the list of DataFrames into one large DataFrame for property data
    new_property_data = pd.concat(new_property_data, ignore_index=True) if new_property_data else pd.DataFrame()
    
    # Concatenate the list of DataFrames into one large DataFrame for valuation data
    new_valuation_data = pd.concat(new_valuation_data, ignore_index=True) if new_valuation_data else pd.DataFrame()

    return new_property_data, new_valuation_data


# Call the function
new_property_data, new_valuation_data = get_updated_data(property_IDs_to_update, BASE_URL, headers)


# Append new data to the existing Property table
Property = pd.concat([Property, new_property_data], ignore_index=True)

# Deduplicate based on 'id', keeping the last occurrence (new data overwrites old data)
Property = Property.drop_duplicates(subset='id', keep='last')







#Update Property table
Property = Property.append(new_property_data)

# Save the DataFrame to a CSV file
new_property_data.to_csv('C:/Users/burit/Documents/VisualStudioCode/DATA/Property.csv', index=False)

new_valuation_data.to_csv('C:/Users/burit/Documents/VisualStudioCode/DATA/Valuation.csv', index=False)






