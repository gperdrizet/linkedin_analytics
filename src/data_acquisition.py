'''Uses LinkedIn analytics export to retreive and extract features from posts for regression analysis'''

import helper_functions as funcs

def make_dataset() -> None:
    '''Retreives and parses posts from most recent LinkedIn analytics export in data/linkedin exports.
    Saves resulting dataset to data/impressions.csv.'''

    # Extract post data from Linkedin export xlxs file
    posts = funcs.parse_linkedin_export()

    # Retreive post content from LinkedIn, add post content string and word count
    posts = funcs.get_posts(posts)