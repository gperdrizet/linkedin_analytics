'''Uses LinkedIn analytics export to retrieve and extract features from posts for regression analysis'''

import os
import helper_functions as funcs

def make_dataset(export_dir: str = None, output_dir: str = None) -> None:
    '''Parses LinkedIn posts from complete_post_history.xlsx. Uses URL to retreive post text from
    LinkedIn. Extracts the following features: impressions, post_text, word_count, n_tags, external_link, media.
    Returns the result as a dataframe.'''

    # Extract post data from Linkedin export xlxs file
    posts = funcs.parse_post_history(data_file)

    # Retrieve post content from LinkedIn, add post content string and word count
    posts = funcs.get_posts(posts)
    
    # Save the processed DataFrame to CSV
    csv_filename = os.path.join(output_dir, 'parsed_posts.csv')
    posts.to_csv(csv_filename, index=False)
    print(f"Processed data saved to: {csv_filename}")


if __name__ == "__main__":

    # Use the current working directory to construct the path to linkedin exports
    current_dir = os.getcwd()
    data_file = os.path.join(current_dir, 'data', 'complete_post_history.xlsx')
    output_path = os.path.join(current_dir, 'data')

    print(f"Current working directory: {current_dir}")

    make_dataset(data_file, output_path)