import advertools as adv
import pandas as pd
import sys
import re

sys.path.append('/Users/burit/Documents/VisualStudioCode/GIT/property_tracker')
from settings import settings

sitemap_url = settings.sitemap_url

def categorize_sitemap(sitemap_df, column, patterns):
    """
    Categorize a column in the DataFrame based on provided patterns.

    Args:
        sitemap_df (pd.DataFrame): The sitemap DataFrame to categorize.
        column (str): The name of the column to create and categorize.
        patterns (dict): A dictionary where keys are category names and values are regex patterns.

    Returns:
        pd.DataFrame: Updated DataFrame with a new categorized column.
    """
    sitemap_df[column] = 'other'  # Default category
    for category, pattern in patterns.items():
        sitemap_df.loc[sitemap_df['sitemap'].str.contains(pattern), column] = category
    return sitemap_df

def extract_region(sitemap):
    """
    Extract region from sitemap URL if available.

    Args:
        sitemap (str): Sitemap URL string.

    Returns:
        str or None: Extracted region or None if not found.
    """
    match = re.search(r'just-sold-([a-zA-Z\-]+)-properties', sitemap)
    return match.group(1) if match else None

def extract_address(url):
    """
    Extract the address from the URL or mark as 'address withheld' if missing.

    Args:
        url (str): URL string to parse.

    Returns:
        str or None: Extracted address or None if no identifiable address is found.
    """
    # Match address in URL after sale, rent, lease, or property
    match = re.search(r'(?:sale|rent|lease|property)/([^/]+)', url)
    if match:
        return match.group(1).replace("-", " ")
    # Mark as withheld if URL ends with sale, rent, or lease without address
    elif re.search(r'(?:sale|rent|lease)/$', url):
        return "address withheld"
    return None

def get_sitemap():
    # Load the sitemap data into a DataFrame and rename columns
    sitemap_df = adv.sitemap_to_df(sitemap_url)[['loc', 'sitemap', 'sitemap_last_modified']]
    sitemap_df.columns = ['url', 'sitemap', 'last_modified']
    
    # Define patterns for listing and property types
    listing_patterns = {
        'insights': 'insights',
        'agent': 'agent',
        'suburbs': 'suburbs',
        'sale_listings': 'sale-listings',
        'just_sold': 'just-sold'
    }
    property_patterns = {
        'residential': 'residential',
        'rural': 'rural',
        'commercial': 'commercial'
    }
    
    # Apply categorizations
    sitemap_df = categorize_sitemap(sitemap_df, 'listing_type', listing_patterns)
    sitemap_df = categorize_sitemap(sitemap_df, 'property_type', property_patterns)

    # Extract regions and addresses
    sitemap_df['region'] = sitemap_df['sitemap'].apply(extract_region)
    sitemap_df['full_address'] = sitemap_df['url'].apply(extract_address)

    # Drop unnecessary rows based on listing_type and sitemap content
    sitemap_df = sitemap_df[~sitemap_df['listing_type'].isin(['insights', 'suburbs', 'agent'])]
    sitemap_df = sitemap_df[~sitemap_df['sitemap'].str.contains('offices|regions|districts')]

    # Save the cleaned and categorized DataFrame to CSV
    sitemap_df.to_csv('C:/Users/burit/Documents/VisualStudioCode/DATA/categorized_sitemap.csv', index=False)

    return sitemap_df
