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
    df['n_tags'] = 0
    df['external_link'] = False
    df['media'] = False
    df['post_day'] = ''
    
    # Process each post in the DataFrame
    for index, row in df.iterrows():

        if index >= 1:
            break

        url = row.get('Post URL', '')
        
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
        
        # Extract day of the week from post publish date
        publish_date = row.get('Post publish date')
        post_day = ''
        if pd.notna(publish_date) and publish_date is not None:
            try:
                # Get the day name (Monday, Tuesday, etc.)
                post_day = publish_date.strftime('%A')
            except (AttributeError, ValueError):
                # Handle cases where the date might not be properly parsed
                post_day = ''
        
        # Update the DataFrame using .loc to ensure proper assignment
        df.loc[index, 'post_text'] = cleaned_content
        df.loc[index, 'word_count'] = word_count
        df.loc[index, 'n_tags'] = n_tags
        df.loc[index, 'external_link'] = external_link
        df.loc[index, 'media'] = has_media
        df.loc[index, 'post_day'] = post_day
        
        print(f"  - Extracted {word_count} words, Tags found: {n_tags}, External link: {external_link}, Media: {has_media}, Day: {post_day}")
    
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
        if 'https://media.licdn.com/dms/image' in content_url:
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