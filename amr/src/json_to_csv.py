import pandas as pd

# Load the JSON file as a Pandas DataFrame
try:
    df = pd.read_json(
        '/Users/wei/Google 雲端硬碟/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antibiotic prescribing/Antibiotic prescribing 01 Jan 2017 - 01 July 2023.json')
except (FileNotFoundError, IOError) as e:
    print("Error: Failed to load the JSON file.")
    print(e)
    exit()
except ValueError as e:
    print("Error: Failed to parse the JSON file.")
    print(e)
    exit()


# Extract captions and URLs
def extract_captions(posts):
    captions = []
    urls = []
    for i, post in enumerate(posts, start=1):
        if 'caption' in post:
            caption = f"{i}. {post['caption']}"
            captions.append(caption)  #don't need to check for duplicate caption
        if 'url' in post:
            url = f"{i}. {post['url']}"
            if url not in urls:  # Check for duplicate URLs
                urls.append(url)
    return captions, urls


df['Caption'], df['URL'] = zip(*df['latestPosts'].apply(extract_captions))

# Save the modified DataFrame to a CSV file
try:
    df.to_csv(
        '/Users/wei/Google 雲端硬碟/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antibiotic prescribing/Antibiotic prescribing 01 Jan 2017 - 01 July 2023.csv',
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
