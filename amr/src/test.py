"""
Aims: To evaluate AMR messaging from Twitter and instagram. To do a content analysis to understand what type of
messages (themes) have been used on social media for AMR from 01 Jan 2017 to 01 July 2023.

pipeline

Step 1. Transfer 10 hashtags files in Json to CSV file

1. AMR
2. Antimicrobial resistance
3. Antibiotics
4. Antimicrobials
5. Antimicrobial stewardship
6. Drug resistant
7. Superbugs
8. Antibiotic resistance
9. Infections
10. Antibiotic prescribing

Step 2. Selected Captions and Urls and drop usefulness headers

Step 3. data cleaning: create a condition to select useful hashtags

Step 4: Merge Instagram and Twitter data

"""

from pathlib import Path  # pathlib: module, Path: class. Checking if a path exist
from typing import Optional, List, Dict, Tuple  # typing: support for type hint
import pandas as pd
from typing import Union


# Load json file
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
        return pd.read_json(p, encoding='utf-8')
        # If the json in the p.name--> read the file. If the string "json" is not in the "name" attribute, this block
        # of code will not execute and the function will return nothing or continue with the next step of code.
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
            urls.append(url)
            # if url not in urls:  # Check for duplicate URLs
            #     urls.append(url)

    return captions, urls


def cleandata(df: pd.DataFrame,
              column_drop: Optional[List[str]] = None,
              keywords_drop: Optional[List[str]] = None,
              save_path: Optional[Path] = None) -> pd.DataFrame:
    df = df.copy()
    if column_drop is not None:
        df = df.drop(columns=column_drop)
    if keywords_drop is not None:
        df = df[df['name'].isin(keywords_drop) == False]
    if save_path is not None:
        df.to_excel(save_path)

    return df


if __name__ == '__main__':
    df = load_json('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/AMR')
    # Print the captions and URLs for easy reference
    df['Caption'], df['URL'] = zip(*df['latestPosts'].apply(extract_captions))

    column_drop = ['id', 'topPostsOnly', 'profilePicUrl', 'postsCount', 'topPosts', 'latestPosts']

    # 1. Infections
    # keywords_drop = ["infectionsurinaires","infectionsofadiffrentkind", "infectionsaypakpunjab",
    #                  "infectionsofadifferentkindpartll","infectionsofadifferentkindstep1",
    #                  "infectionsexuellementtransmissible","infectionsurinaires",
    #                  "infectionsband","infectionssexuellementtransmissibles","infectionsrespiratoires",
    #                  "infectionsvaginales","infectionsportswear","infectionsofadifferentkindstep",
    #                  "infectionsrespiratoires","infectionstore","infections_urinaires","infectionsofdifferentkind"]
    # 2. AMR
    keywords_drop = ["america", "amreading", "captainamerica", "amreli", "americanstaffordshireterrier",
                     "americangirl", "americansalon", "americanbullypocket", "americanbulldog", "americanhistory",
                     "madeinamerica", "copaamerica", "amrezy", "amritsar", "discoversouthamerica", "nativeamerican",
                     "americanpitbull", "makeamericagreatagain", "american", "africanamerican", "proudamerican",
                     "américa", "latinamerica", "amrdiab", "southamerica", "americaneagle", "americanairlines",
                     "americanhorrorstory", "amerika", "americafirst", "americanboy", "americancars",
                     "americanbullies", "americanflag", "americanpitbullterrier", "americalatina", "pastaamericana",
                     "godblessamerica", "capitaoamerica", "amersfoort", "americanstaffordshire", "americasteam",
                     "feriaamericana", "visitsouthamerica", "americanbullyofficial", "americanbullypuppy",
                     "americanbully", "americancar", "americanbullyxl", "amrap", "captainamericacivilwar",
                     "keepamericagreat", "amravati"]
    # 3. Antimicrobial resistance
    # keywords_drop = ["antimicrobialresistanceintanzania",
    #                  "antimicrobialresistanceindonesia",
    #                  "antimicrobialresistancetanzania",
    #                  "antimicrobialresistancemalaysia",
    #                  "antimicrobialresistancemalaysia💊",
    #                  "antimicrobialresistanceis",
    #                  "antimicrobialresistanceinfo➡",
    #                  "antimicrobialresistance✔️",
    #                  "antimicrobialresistancewhat",
    #                  "antimicrobialresistance💊💉",
    #                  "antimicrobialresistance🙏",
    #                  "antimicrobialresistancecontaintment",
    #                  "antimicrobialresistance😉",
    #                  "antimicrobialresistanceisabooboo",
    #                  "antimicrobialresistancecartoonposter",
    #                  "antimicrobialresistanceawarness",
    #                  "antimicrobialresistanceisnotathing",
    #                  "antimicrobialresistanceisscary"]
    # 4. Antibiotics
    # keywords_drop = ['antibioticsmile', 'antibioticskickingin', 'antibioticsftw']
    # 5. Antimicrobials
    # keywords_drop = ['antimicrobialsponge', 'antimicrobials2018', 'antimicrobialsensitivitytesting',
    # 'antimicrobials💉']
    # 6. Antimicrobial stewardship
    # keywords_drop = ["antimicrobialstewardshipwaddup", "antimicrobialstewardshiptraining2019",
    #                  "antimicrobialstewardshiprocks", "antimicrobialstewardchef",
    #                  "antimicrobialstewardshipworkshop2018",
    #                  "antimicrobialstewardahipprogram", "antimicrobialstewardshipinsicilia",
    #                  "antimicrobialstewardship✔", "antimicrobialstewardardship",
    #                  "antimicrobialstewardshipinpediatrics", "antimicrobialstewardshipdinner",
    #                  "antimicrobialstewardshipbrasil", "antimicrobialstewardofgondor",
    #                  "antimicrobialstewardshipprotocol", "antimicrobialstewardshipcertificate",
    #                  "antimicrobialstewardship🧐", "antimicrobialstewardship🎯",
    #                  "antimicrobialstewardaship", "antimicrobialstewardshipconference",
    #                  "antimicrobialstewardshippharmacist", "antimicrobialstewardship🦠",
    #                  "antimicrobialstewardshiprogram", "antimicrobialstewardshipcourse",
    #                  "antimicrobialstewardshipprogrammes", "antimicrobialstewardshipsymposium",
    #                  "antimicrobialstewardship💊", "antimicrobialstewardship2018"]
    # 7. Drug resistant
    # keywords_drop = ["drugresistantbugs", "drugresistantchlamydia", "drugresistantgerms", "drugresistantpathogens",
    #                  "drugresistantuti", "drugresistantstd", "drugresistantbug", "drugresistanthiv",
    #                  "drugresistantecoli", "drugresistantward", "drugresistantinsomnia",
    #                  "drugresistantacinetobacter", "drugresistantcat", "drugresistantnasasusunod",
    #                  "drugresistantcandidaauriscauris", "drugresistanttbcentre", "drugresistantb",
    #                  "drugresistantepilepsysucksevenmore", "drugresistantepilespy", "drugresistant_tuberculosis",
    #                  "drugresistantdepresssion", "drugresistantyak", "drugresistantbacterialinfections",
    #                  "drugresistantaids", "drugresistantfeline", "drugresistantplants😊", "drugresistanttbguidance"]
    # 8. Superbugs
    # keywords_drop = ["superbugsindia", "superbugsy", "superbugster", "superbugsisreal", "superbugsbunny",
    #                  "superbugsunday", "superbugsdepkxd", "superbugslayerspolo", "superbugsboardgame",
    #                  "superbugs1600", "superbugsarereal", "superbugsen", "superbugs23", "superbugs_india",
    #                  "superbugsafari", "superbugsbunnyfunkopop", "superbugshakycam",
    #                  "superbugsize", "superbugs🖋️🔬", "superbugsareassholes", "superbugstotherescue", "superbugsmile",
    #                  "superbugshatecleanhand", "superbugstrikesagain", "superbugsslayers", "superbugsinspace",
    #                  "superbugss", "superbugsandyou", "superbugsと言う無料展示"]
    # 9. Antibiotic resistance
    # keywords_drop = ["antibioticresistanceexplained",
    #                  "antibioticresistancemonth",
    #                  "antibioticresistance💊💉",
    #                  "antibioticresistance⚠️",
    #                  "antibioticresistanceisbad",
    #                  "antibioticresistanceis4real",
    #                  "antibioticresistance👈",
    #                  "antibioticresistanceresearch",
    #                  "antibioticresistanceawarness",
    #                  "antibioticresistancetest",
    #                  "antibioticresistancetesting",
    #                  "antibioticresistanceinindia",
    #                  "antibioticresistanceinchildren",
    #                  "antibioticresistancegenesantibióticos",
    #                  "antibioticresistanceawareness2021",
    #                  "antibioticresistanceisanightmare",
    #                  "antibioticresistancefight",
    #                  "antibioticresistance💊👊",
    #                  "antibioticresistanceontherise",
    #                  "antibioticresistanceofmicrobes"
    #
    #                  ]
    # 10. Antibiotic prescribing :nothing to drop
    # 11.Bacterial infections
    # keywords_drop = ["bacterialinfectionsstink","bacterialinfectionsuck","bacterialinfectionsinchildren",
    #                  "bacterialinfectionsareawesome","bacterialinfectionsepsis","bacterialinfectionsaywhat",
    #                  "bacterialinfectionsaregross","bacterialinfectionsinbotheyes","bacterialinfectionsja",
    #                  "bacterialinfectionsgalore","bacterialinfections", "bacterialinfectionsofskin",
    #                  "bacterialinfectionshavenothingonme","bacterialinfectionsinherstomach",
    #                  "bacterialinfectionsalmostallgone","bacterialinfectionsabound",
    #                  "bacterialinfectionscangetfuckedupthearsebybluewhalesdick","bacterialinfectionsucks",
    #                  "bacterialinfectionscauses","bacterialinfectionsofthe5thdimension","bacterialinfectionsarecool",
    #                  "bacterialinfectionsarenotfun","bacterialinfections😩","bacterialinfectionsoftheskin",
    #                  "bacterialinfectionscantholddisdown","bacterialinfectionsux","bacterialinfectionspeedrun",
    #                  "bacterialinfectionsareabitch","bacterialinfectionse","bacterialinfectionsquad",
    #                  "bacterialinfectionsrising","bacterialinfectionsinhindi","bacterialinfectionsforthewin",
    #                  "bacterialinfectionsarenot","bacterialinfectionsarenojoke",
    #                  "bacterialinfectionsmacterialinfection","bacterialinfectionsintheblood😔💉💊",
    #                  "bacterialinfectionsindogs","bacterialinfectionsfoundhere","bacterialinfectionsarethebest",
    #                  "bacterialinfectionsandsethrogen"]

    save_path = Path(
        '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/AMR/AMR 01 Jan 2017 - 01 July 2023_specific hashtags_test.csv')

    cleaned_df = cleandata(df, column_drop=column_drop, keywords_drop=keywords_drop, save_path=save_path)
    print("Data successfully processed and saved to modified_test.csv.")


def organised_data(df: pd.DataFrame,
                   save_path: Optional[Path] = None) -> pd.DataFrame:
    df['Caption'] = df['Caption'].apply(lambda x: [str(item) for item in x])
    # to convert each element in the 'Caption' column into a list of strings.
    df['URL'] = df['URL'].apply(lambda x: [str(item) for item in x])
    # It's applied to each element x in the 'URL' column. It converts each element into a string using str(item) and
    # places it in a list.

    # Create a new df. each Caption and URL is unique in each cell. But the name ane url keep the same
    new_df = pd.DataFrame({
        'name': df['name'].repeat(df['Caption'].apply(len)),
        'url': df['url'].repeat(df['URL'].apply(len)),
        'Caption': [caption for captions in df['Caption'] for caption in captions],
        # Extracting each element from every column and writing them into individual cells.
        'URL': [url for urls in df['URL'] for url in urls]
    })

    # reset index
    new_df.reset_index(drop=True, inplace=True)

    if save_path:
        new_df.to_csv(save_path)

    return new_df

save_path = (
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/AMR/AMR 01 Jan 2017 - 01 July 2023_test.csv')
organised_data(df, save_path=save_path)
