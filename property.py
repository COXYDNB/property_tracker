from settings import settings
import pandas as pd
import requests
from curl_cffi import requests as cureq
from rich import print
from datetime import datetime
from requests.exceptions import RequestException
import random
import time
from tqdm import tqdm

def get_updated_data(property_IDs_to_update, headers, user_agents):

    # get existing tables
    Property = pd.read_csv(settings.property_table)
    Valuation = pd.read_csv(settings.valuation_table)
    Sale = pd.read_csv(settings.sale_table)

    new_property_data = []  # List to store property data DataFrames
    new_valuation_data = []  # List to store valuation data DataFrames
    new_sale_data = [] # List to store valuation data DataFrames

    retries = 3  # Number of retries for failed requests
    backoff = 2  # Exponential backoff factor

    # Wrap the property ID list with tqdm for a progress bar
    for ID in tqdm(property_IDs_to_update, desc="Processing Properties", unit="property"):
        for attempt in range(retries):
            try:
                # Use a random user-agent for each request to avoid detection
                headers['User-Agent'] = random.choice(user_agents)

                # Send the request
                resp = requests.get(f'{settings.properties_url}{ID}', headers=headers)

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

                    # Append valuation data as DataFrame to the list if confidence is 4+
                    if val_data['confidence_rating'] >= 4:
                        new_valuation_data.append(pd.DataFrame([val_data]))  # Append a single-row DataFrame

            if data['data']['attributes'].get('sales-history', None):
                for sale in data['data']['attributes']['sales-history']:
                    sale_data = {}
                    sale_data['id'] = ID
                    sale_data['sale_date'] = sale.get('sale-date', None)
                    sale_data['net_sale_price'] = sale.get('net-sale-price', None)

                    # Append sale data as DataFrame to the list
                    new_sale_data.append(pd.DataFrame([sale_data]))  # Append a single-row DataFrame

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

    new_sale_data = pd.concat(new_sale_data, ignore_index = True) if new_sale_data else pd.DataFrame()

    # Append new data to the existing Property table
    Property = pd.concat([Property, new_property_data], ignore_index=True)

    # Deduplicate based on 'id', keeping the last occurrence (new data overwrites old data)
    Property = Property.drop_duplicates(subset='id', keep='last')

    # Append new data to the existing Valuation table
    Valuation = pd.concat([Valuation, new_valuation_data], ignore_index=True)

    # Deduplicate based on 'id', 'date' keeping the last occurrence (new data overwrites old data)
    Valuation = Valuation.drop_duplicates(subset=['id', 'estimated_date'], keep='last')

    # Append new data to the existing Sale table
    Sale = pd.concat([Sale, new_sale_data], ignore_index=True)

    # Deduplicate based on 'id', 'date' keeping the last occurrence (new data overwrites old data)
    Sale = Sale.drop_duplicates(subset=['id', 'sale_date'], keep='last')

    # Save the DataFrame to a CSV file
    Property.to_csv(settings.property_table, index=False)

    Valuation.to_csv(settings.valuation_table, index=False)

    Sale.to_csv(settings.sale_table, index=False)

    return new_property_data, new_valuation_data, new_sale_data



