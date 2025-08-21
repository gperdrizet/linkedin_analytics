'''Set of helper functions for downloading and parsing post content.'''

import os
import glob
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from typing import Dict, Any


def parse_linkedin_export() -> dict:
    '''Parser for Linkedin analyicts export xlxs file. Finds most recent export file
    in data/linkedin_exports. Retreives Post URL, Post publish date and Impressions from
    ENGAGEMENT sheet. Returns result as a dictionary.
    '''
    
    # Find the most recent export file in data/linkedin_exports
    export_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'linkedin_exports')
    xlsx_files = glob.glob(os.path.join(export_dir, '*.xlsx'))
    
    if not xlsx_files:
        raise FileNotFoundError("No LinkedIn export files found in data/linkedin_exports/")
    
    # Get the most recent file based on modification time
    most_recent_file = max(xlsx_files, key=os.path.getmtime)
    
    # Read the ENGAGEMENT sheet from the Excel file
    try:
        df = pd.read_excel(most_recent_file, sheet_name='ENGAGEMENT')

    except Exception as e:
        raise ValueError(f"Could not read ENGAGEMENT sheet from {most_recent_file}: {e}")
    
    # Create dictionary with post data
    posts = {}
    
    # Iterate through the dataframe and extract relevant columns
    for idx, row in df.iterrows():
        post_id = f"post_{idx}"
        posts[post_id] = {
            'post_url': row.get('Post URL', ''),
            'publish_date': row.get('Post publish date', ''),
            'impressions': row.get('Impressions', 0)
        }
    
    return posts


def get_posts(posts) -> dict:
    '''Takes dictionary of posts from parse_linkedin_export(), uses requests to retreive
    post text from Post URL. Adds cleaned and normalized text and word count to post
    record in posts dictionary, returns the updated dictionary.'''
    
    def clean_text(text: str) -> str:
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
    
    def extract_post_content(url: str) -> str:
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
    
    # Process each post in the dictionary
    for post_id, post_data in posts.items():
        url = post_data.get('post_url', '')
        
        # Extract post content from URL
        raw_content = extract_post_content(url)
        
        # Clean and normalize the text
        cleaned_content = clean_text(raw_content)
        
        # Calculate word count
        word_count = len(cleaned_content.split()) if cleaned_content else 0
        
        # Add the new fields to the post data
        post_data['post_text'] = cleaned_content
        post_data['word_count'] = word_count
        
        # Optional: Add original raw content for debugging
        post_data['raw_text'] = raw_content
    
    return posts