import pandas as pd
import sys
pd.set_option('display.max_colwidth',None)

sys.path.append('/Users/burit/Documents/VisualStudioCode/GIT/property_tracker')

import sitemap

# Get SiteMap DF and write to csv 'C:/Users/burit/Documents/VisualStudioCode/DATA/categorized_sitemap.csv'
urls = sitemap.get_sitemap()

testurl = urls.loc[urls['listing_type'] == 'just_sold', 'url'].tail()
len(urls.loc[urls['listing_type'] == 'just_sold'])


## Scrape for prices, rent prices, lease prices, bedrooms, bathrooms, region, sub-region, city, street, sold date, car parking, floor space m**2, property area **2 (size), property history?

## Need to record IDs, understand that properties are listed more than once, e.g. sold and then rented, sold multiple times.

## Store property data which can be updated over time, to maintain history - how do we model this??

## Some kind of API to get location data e.g. latitude longitude, what kind of spatial data is needed for powerBI / GIS maps??
    ## https://addressfinder.nz/api/nz/location/metadata/

## FUTURE - model time series - regional mean sale price by factors