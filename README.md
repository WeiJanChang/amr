# AMR

This project aims to categorize and evaluate images, videos, and posts related to antimicrobial resistance (AMR) on
Instagram, using the Instagram API via Instaloader.

## Installation

- Create an environment for the required dependencies

```
conda create -n [ENV_NAME] python ~=3.10
conda activate [ENV_NAME]
cd [CLONED_DIRECTORY]
pip install -r requirements.txt  
```

## Flowchart

![Example 2](figure/flowchart.png)

- Use the [Apify Scraper](https://console.apify.com/actors/shu8hvrXbJbY3Eb9W/input) to extract data based on
  research-specific keywords, and save the results as JSON files.
- Combine the JSON files (one per keyword), and extract the unique post IDs.
- Select meaningful images that effectively convey health-related information.
- Assign images to different categories

## Usage

### Pre-processing

- After extracting data and obtaining JSON files using Apify, read the directory containing the downloaded JSON files,
  remove unused columns and duplicate post IDs, and save the result as a DataFrame.

```
    from Ig_info import LatestPostInfo, load_from_directory
    d = '[PLEASE ADD YOUR SYSTEM PATH with CORRESPONDING DIRECTORIES]
    ify = load_from_directory(d) # read the JSON files in the directory
    info = [it.collect_latest_posts() for it in ify]
    ret = LatestPostInfo.concat(info)  # concat json files
    df = ret.remove_unused_fields().remove_duplicate().to_dataframe()
```

- Next, start downloading images and/or videos, and save any error messages to a CSV file if a download fails.

```
    d = '[PLEASE ADD YOUR SYSTEM PATH with CORRESPONDING DIRECTORIES]
    info = create_latestpost_info(d)
    output_path = '[OUTPUT PATH]'
    error_out = '[OUTPUT PATH]'
    download_image(info, output_path=output_path, error_out=error_out)
```

## Data Visualisation

![Example 1](figure/wordcloud.png)

## Contact

Wei Jan Chang, weijan.chang@gmail.com