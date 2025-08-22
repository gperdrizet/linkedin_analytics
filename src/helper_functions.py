'''Set of helper functions for downloading and parsing post content.'''

import glob
import os
import random
import re
import time
import warnings
from typing import Dict, Any

import pandas as pd
import requests
from bs4 import BeautifulSoup



def parse_post_history(data_file: str) -> pd.DataFrame:
    '''Parser for Linkedin posts xlxs file. Retrieves Post URL, and Impressions.
    Returns result as a dataframe.
    '''
    
    try:

        # Suppress warnings from pandas
        with warnings.catch_warnings():
            warnings.simplefilter('ignore') 
            df = pd.read_excel(data_file, sheet_name='Sheet1')
            print(df.head())

    except Exception as e:
        raise ValueError(f'Could not read Sheet1 sheet from {data_file}: {e}')

    # Select columns
    df = df[['impressions', 'post_url']]

    # Clean up the index
    df.reset_index(drop=True, inplace=True)

    # Convert impressions column to integer
    if 'impressions' in df.columns:
        df['impressions'] = pd.to_numeric(df['impressions'], errors='coerce').astype('Int64')
    
    # Check the column names to ensure the parse was successful
    if ','.join(df.columns) == 'impressions,post_url':
        print(df.head())
        return df

    else:
        raise ValueError('Unexpected column names found in input file export.')


def get_posts(posts_df: pd.DataFrame) -> pd.DataFrame:
    '''Takes DataFrame of posts from parse_linkedin_export(), uses requests to retrieve
    post text from Post URL. Adds cleaned and normalized text and word count to post
    record in posts DataFrame, returns the updated DataFrame.'''
    
    # Create a copy of the DataFrame to avoid modifying the original
    df = posts_df.copy()
    
    # Initialize new columns
    df['post_text'] = ''
    df['word_count'] = 0
    df['n_tags'] = 0
    df['external_link'] = False
    df['media'] = False
    
    # Process each post in the DataFrame
    for index, row in df.iterrows():

        url = row.get('post_url', '')
        url = url.split('?')[0]
        
        print(f"Processing post {index + 1}/{len(df)}: {url}")
        
        # Download post HTML
        html_content = _download_post_html(url)
        
        # Extract post text content from HTML
        raw_text_content = _extract_post_content(html_content)

        # Check for tags (users or companies) in the raw content
        n_tags = _detect_tags(raw_text_content)
        
        # Check for external links (LinkedIn's lnkd.in redirector)
        external_link = _detect_external_link(raw_text_content)

        # Check for media content using the HTML
        has_media = _detect_media(html_content)

        # Clean and normalize the text
        cleaned_content = _clean_text(raw_text_content)
        
        # Calculate word count
        word_count = len(cleaned_content.split()) if cleaned_content else 0
        
        # Update the DataFrame using .loc to ensure proper assignment
        df.loc[index, 'post_text'] = cleaned_content
        df.loc[index, 'word_count'] = word_count
        df.loc[index, 'n_tags'] = n_tags
        df.loc[index, 'external_link'] = external_link
        df.loc[index, 'media'] = has_media
        
        print(f"  - Extracted {word_count} words, Tags found: {n_tags}, External link: {external_link}, Media: {has_media}")
        
        # Add random sleep to prevent hitting the site too hard
        sleep_time = random.uniform(2, 5)  # Random sleep between 2-5 seconds
        print(f"  - Sleeping for {sleep_time:.1f} seconds...")
        time.sleep(sleep_time)
    
    print(f"\nProcessed {len(df)} posts successfully")
    
    # Clean up column names: replace spaces with underscores and make lowercase
    df.columns = df.columns.str.replace(' ', '_').str.lower()

    return df


def _clean_text(text: str) -> str:
    '''Clean and normalize text by removing extra whitespace, special characters, and external links.'''

    if not text:
        return ""
    
    # Remove HTML tags if any
    text = BeautifulSoup(text, 'html.parser').get_text()
    
    # Remove LinkedIn external links (lnkd.in redirects)
    # Pattern matches various forms: https://lnkd.in/..., httpslnkd.in..., lnkd.in/...
    text = re.sub(r'https?://lnkd\.in/[a-zA-Z0-9_-]+', '', text)  # Standard format
    text = re.sub(r'httpslnkd\.in[a-zA-Z0-9_-]+', '', text)  # Mangled format without colon/slash
    text = re.sub(r'lnkd\.in/[a-zA-Z0-9_-]+', '', text)  # Short format
    
    # Remove other common external link patterns
    text = re.sub(r'https?://[^\s]+', '', text)  # Remove any remaining URLs
    
    # Remove extra whitespace and normalize
    text = re.sub(r'\s+', ' ', text.strip())
    
    # Clean up floating punctuation by removing preceding whitespace
    text = re.sub(r'\s+([.!?,:;])', r'\1', text)  # Remove space before punctuation
    
    # Remove special characters but keep basic punctuation and forward slashes
    text = re.sub(r'[^\w\s.,!?/-]', '', text)
    
    return text

def _detect_tags(raw_content: str) -> int:
    '''Count hashtags in the raw post content.
    
    Returns the number of hashtags (# symbols prepended to strings) found.
    '''
    
    if not raw_content:
        return 0
    
    # Parse the raw content with BeautifulSoup to get clean text
    soup = BeautifulSoup(raw_content, 'html.parser')
    text_content = soup.get_text()
    
    # Look for hashtag pattern: # followed by word characters
    hashtag_pattern = r'#\w+'
    matches = re.findall(hashtag_pattern, text_content)
    
    return len(matches)


def _detect_external_link(raw_content: str) -> bool:
    '''Detect if external links are present in the raw post content.
    
    Returns True if LinkedIn's external link redirector (https://lnkd.in) is found, False otherwise.
    LinkedIn uses lnkd.in to redirect to external websites.
    '''
    
    if not raw_content:
        return False
    
    # Check for LinkedIn's external link redirector
    return 'https://lnkd.in' in raw_content


def _detect_media(html_content: str) -> bool:
    '''Detect if media content is present in the raw post HTML.
    
    Returns True if an og:image meta tag with a LinkedIn media URL is found, False otherwise.
    This specifically looks for LinkedIn's image sharing system.
    '''
    
    if not html_content:
        return False
    
    # Parse the raw content with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Look for og:image meta tag with LinkedIn media URL
    og_image = soup.find('meta', property='og:image')
    if og_image:
        content_url = og_image.get('content', '')
        print(content_url)
        if re.match(r"https://media.licdn.com/dms/image/sync/v2/.+/articleshare", content_url) or 'https://static.licdn.com/aero-v1' in content_url:
            return True
    
    return False


def _download_post_html(url: str) -> str:
    '''Download HTML content from LinkedIn post URL.
    
    Returns the raw HTML content as a string, or empty string if download fails.
    '''
    
    if not url:
        return ""
    
    try:
        # Set up headers to mimic a real browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        return response.text
        
    except requests.RequestException as e:
        print(f"Error fetching content from {url}: {e}")
        return ""
    
    except Exception as e:
        print(f"Error downloading from {url}: {e}")
        return ""


def _extract_post_content(html_content: str) -> str:
    '''Extract post text content from LinkedIn post HTML.
    
    Takes raw HTML content and extracts the main post text from the meta description tag.
    '''
    
    if not html_content:
        return ""
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Look for meta tag with name="description" in the head section
        description_meta = soup.find('meta', attrs={'name': 'description'})
        if description_meta:
            content = description_meta.get('content', '')
            if content:
                return content.strip()
        
        return ""
        
    except Exception as e:
        print(f"Error parsing HTML content: {e}")
        return ""