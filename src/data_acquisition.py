'''Uses LinkedIn analytics export to retrieve and extract features from posts for regression analysis'''

import os
import helper_functions as funcs

def make_dataset(export_dir: str = None, output_dir: str = None) -> None:
    '''Retrieves and parses posts from most recent LinkedIn analytics export in data/linkedin exports.
    Saves resulting dataset to data/impressions.csv.
    
    Args:
        export_dir: Path to the directory containing LinkedIn export files.

    '''

    # Extract post data from Linkedin export xlxs file
    posts = funcs.parse_linkedin_export(export_dir)

    # Retrieve post content from LinkedIn, add post content string and word count
    posts = funcs.get_posts(posts)
    
    # Save the processed DataFrame to CSV
    csv_filename = os.path.join(output_dir, 'posts.csv')
    posts.to_csv(csv_filename, index=False)
    print(f"Processed data saved to: {csv_filename}")


if __name__ == "__main__":

    # Use the current working directory to construct the path to linkedin exports
    current_dir = os.getcwd()
    export_path = os.path.join(current_dir, 'data', 'linkedin_exports')
    output_path = os.path.join(current_dir, 'data')

    print(f"Current working directory: {current_dir}")

    make_dataset(export_path, output_path)