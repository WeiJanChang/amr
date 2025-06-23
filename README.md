# AMR

This project aims to categorize and evaluate images, videos, and posts related to antimicrobial resistance (AMR) on
Instagram, using the Instagram API via Instaloader.

## Installation

- Create an environment for the required dependencies

```
conda create -n [ENV_NAME] python =3.10
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
- Assign meaningful images to pre-defined
  categories ([Reference - COVID-19 Public Health Communication on X (Formerly Twitter): Cross-Sectional Study of Message Type, Sentiment, and Source](https://formative.jmir.org/2025/1/e59687))

## Usage

### Pre-processing

- After extracting data and obtaining JSON files using Apify, read the directory containing the downloaded JSON files,
  remove unused columns and duplicate post IDs, and save the result as a DataFrame.

```
    from Ig_info import LatestPostInfo, load_from_directory
    # # Set the directory path to your local JSON files
    d = 'your/json/folder/path'
    ify = load_from_directory(d) 
    info = [it.collect_latest_posts() for it in ify]
    ret = LatestPostInfo.concat(info)  # concat json files
    df = ret.remove_unused_fields().remove_duplicate().to_dataframe()
```

- Next, start downloading images and/or videos, and save any error messages to a CSV file if a download fails.

```
    d =  'your/json/folder/path'
    info = create_latestpost_info(d)
    output_path = 'your/output/image/folder'
    error_out = your/error/log.csv'
    download_image(info, output_path=output_path, error_out=error_out)
```

### Post-processing

#### Image Text Extraction

If you need to extract text from .jpg images automatically, use the `extract_text_from_image.py` script.

- Modify the directory variable in the script to point to your folder containing .jpg images.
- The script will process each image, extract text and save the results as a CSV file

## Statistics

### Descriptive statistics

The descriptive_stats function displays descriptive statistics (count and percentage) for a given column in a DataFrame.
You can modify the grouping column(s) as needed depending on your analysis goals.

```
    from descriptive_stats import descriptive_stats
    df = pd.read_excel('~/code/amr/test_file/post_processed_data.xlsx')
    descriptive_stats(df, 'likesCount', groupby_col=['cat', 'year'])
    descriptive_stats(df,col_name='cat')
```

### Cohen’s kappa

Use this script to calculate Cohen’s kappa, a statistical measure of inter-annotator agreement.

- coder_1 and coder_2 are flexible: just pass the column names of any two coders you want to compare.
- Cohen’s kappa value ranges from -1 to 1. Values closer to 1 indicate strong agreement.

```    
df = pd.read_excel("~/code/amr/test_file/coders_messages.xlsx")
cal_kappa(df, coder_1='coder1', coder_2='coder2')
```

## Data Visualisation

![Example 1](figure/wordcloud.png)

## Contact

Wei Jan Chang, weijan.chang@gmail.com