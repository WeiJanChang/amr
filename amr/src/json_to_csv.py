import pandas as pd
from pathlib import Path
from typing import Union


# Load the JSON file as a Pandas DataFrame
def load_json(p: Union[Path, str]) -> pd.DataFrame:
    """
    load json file

    :param p: json path or containing folder
    :return:
        pd.DataFrame
    """
    if isinstance(p, str):  # if the variable p is an instance of the str class
        p = Path(p)  # if yes, creates a new object of 'Path' class and assigns it to the variable 'p'

    if 'json' in p.name:
        return pd.read_json(p,
                            encoding='utf-8')  # If the 'json in the p.name--> read this json file. If the string "json"
        # is not in the "name" attribute, this block of code will not execute and the function will return nothing
        # or continue with the next step of code.

    else:
        f = list(
            p.glob('*.json'))  # To check if there is any json file present in the path p or not by using glob method
        # and return a list of all the json files stored in 'p'
        if len(f) == 0:
            raise FileNotFoundError(f'no json file under the {p}')
        elif len(f) == 1:
            return pd.read_json(f[0], encoding='utf-8')
        else:
            raise RuntimeError(f'multiple json files under the {p}')


df = load_json('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antibiotic resistance')


# Extract captions and URLs
def extract_captions(posts):
    captions = []
    urls = []
    for i, post in enumerate(posts, start=1):
        if 'caption' in post:
            caption = f"{i}. {post['caption']}"
            captions.append(caption)  # don't need to check for duplicate caption
        if 'url' in post:
            url = f"{i}. {post['url']}"
            if url not in urls:  # Check for duplicate URLs
                urls.append(url)
    return captions, urls


df['Caption'], df['URL'] = zip(*df['latestPosts'].apply(extract_captions))

# Save the modified DataFrame to a CSV file
try:
    df.to_csv(
        '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antibiotic resistance/Antibiotic resistance 01 Jan 2017 - 01 July 2023.csv',
        index=False)
except (FileNotFoundError, IOError) as e:
    print("Error: Failed to save the DataFrame to CSV.")
    print(e)
    exit()

# Print the captions and URLs for easy reference
for i, (captions, urls) in enumerate(zip(df['Caption'], df['URL']), start=1):
    print(f"Post {i}:")
    for caption in captions:
        print(f"  Caption: {caption}")
    for url in urls:
        print(f"  URL: {url}")
    print()

# Print a success message
print("Data successfully processed and saved to modified_test.csv.")
