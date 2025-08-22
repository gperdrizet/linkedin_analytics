# LinkedIn Analytics

A Python tool for analyzing LinkedIn post performance by extracting and processing post data from LinkedIn analytics exports.

## Overview

This project helps you analyze your LinkedIn posts by:
- Parsing LinkedIn analytics export files
- Extracting post content, engagement metrics, and metadata
- Analyzing post features like word count, tags, media presence, and posting patterns
- Saving processed data for further analysis

## Features

- **Post Content Extraction**: Downloads and parses post text from LinkedIn URLs
- **Feature Engineering**: Extracts various features from posts including:
  - Word count and cleaned text content
  - Number of tagged users/companies
  - External link detection
  - Media content detection (images, videos, documents)
  - Day of the week posted
- **Data Processing**: Cleans and normalizes text content
- **Export Handling**: Automatically finds and processes the most recent LinkedIn export file

## Requirements

See `requirements.txt` for Python dependencies.

## Usage

1. Export your LinkedIn analytics data and place the `.xlsx` file in `data/linkedin_exports/`
2. Run the data acquisition script:
   ```bash
   python src/data_acquisition.py
   ```
3. Find your processed data in `data/processed/processed_posts.csv`

## Project Structure

```
├── src/
│   ├── data_acquisition.py    # Main script for processing LinkedIn exports
│   └── helper_functions.py    # Core functions for data extraction and processing
├── data/
│   ├── linkedin_exports/      # Place your LinkedIn export files here
│   └── processed/            # Processed CSV files output here
└── models/                   # (Future: ML models for analysis)
```

## Data Privacy

This tool processes your personal LinkedIn data locally. No data is sent to external services except for downloading public post content from LinkedIn URLs.

## Contributing

This is a personal analytics project, but suggestions and improvements are welcome!
