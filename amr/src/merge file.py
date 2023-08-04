import pandas as pd

"""
merge Twitter data (IE, GB, and both)

"""


df1 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/AMR/AMR 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
df2 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antibiotic prescribing/Antibiotic prescribing 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
df3 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antibiotic resistance/Antibiotic resistance 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
df4 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antibiotics/Antibiotics 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
df5 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance/Antimicrobial resistance 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
df6 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial stewardship/Antimicrobial stewardship 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
df7 =pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobials/Antimicrobials 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
df8 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Bacterial infections/Bacterial infections 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
df9 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Drug resistant/Drug resistant 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
df10 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Infections/Infections 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
df11 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Superbugs/Superbugs 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')

instagram_df = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11])
instagram_df.to_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/all_Instagram_data(non-English excluded).csv',
                index=False)



num_rows = instagram_df.shape[0]
num_cols = instagram_df.shape[1]

print("row and col：", instagram_df.shape)



