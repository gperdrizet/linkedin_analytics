'''Set of helper functions for downloading and parsing post content.'''

import glob
import os
import re
import warnings
from typing import Dict, Any

import pandas as pd
import requests
from bs4 import BeautifulSoup



def parse_linkedin_export(export_dir: str) -> pd.DataFrame:
    '''Parser for Linkedin analytics export xlxs file. Finds most recent export file
    in data/linkedin_exports. Retrieves Post URL, Post publish date and Impressions from
    TOP POSTS sheet. Returns result as a dataframe.
    '''
    
    # Find the most recent export file in data/linkedin_exports
    xlsx_files = glob.glob(os.path.join(export_dir, '*.xlsx'))

    print(f'Export directory: {export_dir}')
    print(f'Found {len(xlsx_files)} files: {xlsx_files}')

    if not xlsx_files:
        raise FileNotFoundError('No LinkedIn export files found in data/linkedin_exports/')
    
    # Get the most recent file based on modification time
    most_recent_file = max(xlsx_files, key=os.path.getmtime)
    
    # Read the TOP POSTS sheet from the Excel file
    try:

        # Suppress warnings from pandas
        with warnings.catch_warnings():
            warnings.simplefilter('ignore') 
            df = pd.read_excel(most_recent_file, sheet_name='TOP POSTS')

    except Exception as e:
        raise ValueError(f'Could not read TOP POSTS sheet from {most_recent_file}: {e}')

    # Select impressions table columns
    df = df[['Unnamed: 4', 'Unnamed: 5', 'Unnamed: 6']]

    # The third row in the dataframe contains the column headers
    df.columns = df.iloc[1]

    # Get rid of the rest of the unnecessary rows
    df = df.iloc[2:]

    # Clean up the index
    df.reset_index(drop=True, inplace=True)
    
    # Convert Impressions column to integer
    if 'Impressions' in df.columns:
        df['Impressions'] = pd.to_numeric(df['Impressions'], errors='coerce').astype('Int64')
    
    # Convert Post publish date to datetime
    if 'Post publish date' in df.columns:
        df['Post publish date'] = pd.to_datetime(df['Post publish date'], errors='coerce')
    
    # Check the column names to ensure the parse was successful
    if ','.join(df.columns) == 'Post URL,Post publish date,Impressions':
        print(df.head())
        return df

    else:
        raise ValueError('Unexpected column names found in LinkedIn export.')


def get_posts(posts_df: pd.DataFrame) -> pd.DataFrame:
    '''Takes DataFrame of posts from parse_linkedin_export(), uses requests to retrieve
    post text from Post URL. Adds cleaned and normalized text and word count to post
    record in posts DataFrame, returns the updated DataFrame.'''
    
    # Create a copy of the DataFrame to avoid modifying the original
    df = posts_df.copy()
    
    # Initialize new columns
    df['post_text'] = ''
    df['word_count'] = 0
    df['raw_text'] = ''
    df['n_tags'] = 0
    df['external_link'] = False
    
    # Process each post in the DataFrame
    for index, row in df.iterrows():

        if index >= 1:
            break

        url = row.get('Post URL', '')
        
        print(f"Processing post {index + 1}/{len(df)}: {url}")
        
        # Extract post content from URL
        raw_content = _extract_post_content(url)
        
        # Clean and normalize the text
        cleaned_content = _clean_text(raw_content)
        
        # Calculate word count
        word_count = len(cleaned_content.split()) if cleaned_content else 0
        
        # Check for tags (users or companies) in the raw content
        n_tags = _detect_tags(raw_content)
        
        # Check for external links (LinkedIn's lnkd.in redirector)
        external_link = _detect_external_link(raw_content)
        
        # Update the DataFrame using .loc to ensure proper assignment
        df.loc[index, 'post_text'] = cleaned_content
        df.loc[index, 'word_count'] = word_count
        df.loc[index, 'raw_text'] = raw_content
        df.loc[index, 'n_tags'] = n_tags
        df.loc[index, 'external_link'] = external_link
        
        print(f"  - Extracted {word_count} words, Tags found: {n_tags}, External link: {external_link}")
    
    print(f"\nProcessed {len(df)} posts successfully")

    return df


def _clean_text(text: str) -> str:
    '''Clean and normalize text by removing extra whitespace and special characters.'''

    if not text:
        return ""
    
    # Remove HTML tags if any
    text = BeautifulSoup(text, 'html.parser').get_text()
    
    # Remove extra whitespace and normalize
    text = re.sub(r'\s+', ' ', text.strip())
    
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s.,!?-]', '', text)
    
    return text

def _detect_tags(raw_content: str) -> int:
    '''Count users, companies, and hashtags tagged in the raw post content.
    
    Returns the total number of LinkedIn tags found.
    LinkedIn tags include @mentions, hashtags (#), and HTML-based user/company references.
    '''
    
    if not raw_content:
        return 0
    
    tag_count = 0
    
    # Parse the raw content with BeautifulSoup
    soup = BeautifulSoup(raw_content, 'html.parser')
    
    # Look for common LinkedIn tag patterns in HTML
    tag_indicators = [
        # LinkedIn user/company mentions often have these classes or attributes
        'a[href*="/in/"]',  # User profile links
        'a[href*="/company/"]',  # Company profile links
        '.feed-shared-actor__name',  # Actor name mentions
        '.feed-shared-actor__title',  # Actor title mentions
        '[data-entity-urn*="person"]',  # Person entity URNs
        '[data-entity-urn*="company"]',  # Company entity URNs
        '.mention',  # General mention class
        '.tagged-mention',  # Tagged mention class
    ]
    
    # Check for HTML-based tag indicators
    for selector in tag_indicators:
        elements = soup.select(selector)
        tag_count += len(elements)
    
    # Check for @mentions and hashtags in text (fallback for text-based detection)
    text_content = soup.get_text()
    
    # Look for @username and hashtag patterns (basic pattern matching)
    import re
    mention_patterns = [
        r'@[a-zA-Z0-9._-]+',  # Basic @username pattern
        r'@\w+\s+\w+',  # @FirstName LastName pattern
        r'#\w+',  # Hashtag pattern (very common on LinkedIn)
    ]
    
    for pattern in mention_patterns:
        matches = re.findall(pattern, text_content)
        tag_count += len(matches)
    
    # Look for phrases that typically indicate tagging
    tag_phrases = [
        'tagged',
        'mentioned',
        'thanks to',
        'kudos to',
        'shout out to',
        'thanks for',
    ]
    
    text_lower = text_content.lower()
    for phrase in tag_phrases:
        if phrase in text_lower:
            tag_count += 1
    
    return tag_count

def _detect_external_link(raw_content: str) -> bool:
    '''Detect if external links are present in the raw post content.
    
    Returns True if LinkedIn's external link redirector (https://lnkd.in) is found, False otherwise.
    LinkedIn uses lnkd.in to redirect to external websites.
    '''
    
    if not raw_content:
        return False
    
    # Check for LinkedIn's external link redirector
    return 'https://lnkd.in' in raw_content

def _extract_post_content(url: str) -> str:
    '''Extract post content from LinkedIn post URL.'''
    
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
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Try different selectors to find post content
        content_selectors = [
            '.feed-shared-text__text-view',
            '.feed-shared-update-v2__commentary',
            '.feed-shared-text',
            'span[dir="ltr"]',
            '.break-words'
        ]
        
        for selector in content_selectors:
            elements = soup.select(selector)
            if elements:
                # Get text from all matching elements and join them
                text_parts = [elem.get_text().strip() for elem in elements if elem.get_text().strip()]
                if text_parts:
                    return ' '.join(text_parts)
        
        # Fallback: try to find any text content in common post areas
        post_areas = soup.find_all(['div', 'span'], class_=lambda x: x and any(
            keyword in x.lower() for keyword in ['text', 'content', 'commentary', 'post']
        ))
        
        for area in post_areas:
            text = area.get_text().strip()
            if text and len(text) > 20:  # Assume meaningful content is longer than 20 chars
                return text
        
        return ""
        
    except requests.RequestException as e:
        print(f"Error fetching content from {url}: {e}")
        return ""

    except Exception as e:
        print(f"Error parsing content from {url}: {e}")
        return ""