# import packages and modules --------------------------------------------------------------

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
import region
import property
import id

# API request setup ------------------------------------------------------------------------

headers = {"User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0"}

# Sample user agents
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0"
]

# GET SITEMAP, Property --------------------------------------------------------------------

sitemap_df = sitemap.get_sitemap()

# REFRESH REGION CODES ---------------------------------------------------------------------

#region_codes = region.get_region_IDs(headers, user_agents)
#region_codes.to_csv(settings.region_table, index = False)

# READ STORED REGION CODES -----------------------------------------------------------------

region_codes = pd.read_csv(settings.region_table)
# remove 'confidential' region
region_codes = region_codes[region_codes['region'] != "Confidential"]

# Regional FULL LOAD ----------------------------------------------------------------------- # comment out this section once a full load has been completed for each region
# Change this to choose which regions to update
RegionNames = region_codes['region_slug'].iloc[0:2]

property_IDs_to_update = id.get_all_properties_by_region(sitemap_df, RegionNames, headers, user_agents)

# retrieve and load the property information for the properties that were recently updated
property.get_updated_data(property_IDs_to_update, headers, user_agents)

# GET DATA FOR NEWLY UPDATED PROPERTIRES ---------------------------------------------------
# Define search for finding newly updated properties
regionIDs = region_codes['regionID'].iloc[0:2]
start_date = datetime.today() - timedelta(weeks=1)
start_date = start_date.strftime('%Y-%m-%d')
page_limit = 1000

# Get properties that were recently updated, and not loaded in the last week
properties_to_update = id.get_recently_updated_propeties(regionIDs, start_date, page_limit, headers, user_agents)
# get property IDs of properties that were recently updated
property_IDs_to_update = properties_to_update['propertyID']

# retrieve and load the property information for the properties that were recently updated
property.get_updated_data(property_IDs_to_update, headers, user_agents)