from settings import settings
import pandas as pd
import requests
from curl_cffi import requests as cureq
from rich import print
from datetime import datetime, timedelta
import random
import time

def get_recently_updated_propeties(regionIDs, start_date, page_limit, headers, user_agents):

    # Store region codes
    recently_updated_properties = []

    # Loop through IDs
    for regionID in regionIDs:
        # Format the URL
        URL = f'https://platform.realestate.co.nz/search/v1/properties?filter%5Bregion%5D%5B%5D={regionID}&filter%5BsaleDateMin%5D={start_date}&filter%5BpropertyTypes%5D%5B%5D=Residential%20Section&meta%5Baggs%5D=propertyType%2Cregion&page%5Boffset%5D=0&page%5Blimit%5D={page_limit}'

        # Use a random user-agent for each request
        headers["User-Agent"] = random.choice(user_agents)

        try:
            # Send the request
            resp = requests.get(URL, headers=headers)

            # Handle non-200 responses
            if resp.status_code != 200:
                continue

            # Parse JSON response
            data = resp.json()

            # Extract region information
            if data['data']:
                for property in data['data']:
                    search_data = {}
                    search_data['regionID'] = regionID
                    search_data['region'] = property['attributes']['address'].get('region', None)
                    search_data['propertyID'] = property['attributes'].get('short-id', None)

                    # Append region data to the list
                    recently_updated_properties.append(search_data)
        except Exception as e:
            print(f"Error processing region ID {regionID}: {e}")

        # Optional: Pause between requests
        time.sleep(random.uniform(1, 3))  # Random sleep between 0.8-2 seconds

    # Convert region codes to a DataFrame
    recently_updated_properties_df = pd.DataFrame(recently_updated_properties)

    # Filter rows where 'last_update' is older than 1 week
    Property_updates = pd.read_csv(settings.property_table)[['id', 'last_update']]

    recently_updated_properties_df_merge = recently_updated_properties_df.merge(Property_updates, how = "left", left_on = "propertyID", right_on = "id")[['regionID', 'region', 'propertyID', 'last_update']]

    #Ideally, this is removed. I will need to ensure I can get a full load for each region within a week, and then can rely on the GET_UPDATED_PROPERTIES
    #recently_updated_properties_df_filtered = recently_updated_properties_df_merge[
    #pd.to_datetime(recently_updated_properties_df_merge['last_update']).dt.date < start_date
    #]

    #print(f'Properties to update: {recently_updated_properties_df_filtered.shape[0]}')

    print(f'Properties to update: {recently_updated_properties_df_merge.shape[0]}')

    #return recently_updated_properties_df_filtered
    return recently_updated_properties_df_merge


# test the function
#recently_updated_properties_df = get_recently_updated_propeties(regionIDs, start_date, page_limit, headers, user_agents)

def get_all_properties_by_region(sitemap_df, RegionNames, headers, user_agents):

    # Filter the dataframe for sold properties, residential type, and the specified regions
    filtered_df = sitemap_df[(sitemap_df['listing_type'] == 'sold') & 
                              (sitemap_df['property_type'] == 'residential') & 
                              (sitemap_df['region'].isin(RegionNames))]

    # Extract the property IDs from the filtered dataframe
    sitemap_sold_property_IDs = filtered_df['ID']

    # Convert sitemap_sold_property_IDs into a DataFrame with the column 'id' for merging
    sitemap_sold_property_IDs_df = sitemap_sold_property_IDs.to_frame(name='id')

    Property_updates = pd.read_csv(settings.property_table)[['id', 'last_update']]

    # Perform an outer join
    Property_IDs_Dates = pd.merge(Property_updates, sitemap_sold_property_IDs_df, on='id', how='outer')

    # Replace null (NaT) values in 'last_update' with the default date '1900-01-01'
    Property_IDs_Dates['last_update'] = Property_IDs_Dates['last_update'].fillna(pd.to_datetime('1900-01-01'))

    # Convert 'last_update' to datetime for comparison
    Property_IDs_Dates['last_update'] = pd.to_datetime(Property_IDs_Dates['last_update'])

    # Get the date of one week ago from today
    one_week_ago = datetime.today() - timedelta(weeks=1)

    x1 = Property_IDs_Dates.shape

    # Filter properties that have not been updated in the last week
    Properties_not_updated_recently = Property_IDs_Dates[Property_IDs_Dates['last_update'] < one_week_ago]

    x2 = Properties_not_updated_recently.shape

    print(f'{x1[0]} properties to begin with...')
    print(f'{x2[0]} properties to update...')

    property_IDs_to_update = Properties_not_updated_recently['id']

    return property_IDs_to_update