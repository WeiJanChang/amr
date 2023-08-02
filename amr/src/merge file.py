"""
merge Twitter data (IE, GB, and both)

"""
import pandas as pd
#
# df1 = pd.read_csv(
#     '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Twitter data/Superbugs/Superbugs GB 01-17 to 01-23.csv')
# df2 = pd.read_csv(
#     '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Twitter data/Infections/Infections GB 01-17 to 01-23.csv')
# df3 = pd.read_csv(
#     '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Twitter data/Drug resistant/Drug resistant GB 01-17 to 01-23.csv')
# df4 = pd.read_csv(
#     '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Twitter data/Antimicrobials/Antimicrobials GB 01-17 to 01-23.csv')
# df5 = pd.read_csv(
#     '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Twitter data/Antimicrobial stewardship/Antimicrobial stewardship GB 01-17 to 01-23.csv')
# df6 = pd.read_csv(
#     '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Twitter data/Antimicrobial resistance/Antimicrobial resistance GB 01-17 to 01-23.csv')
# df7 = pd.read_csv(
#     '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Twitter data/Antibiotics/Antibiotics GB 01-17 to 01-23.csv')
# df8 = pd.read_csv(
#     '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Twitter data/Antibiotic prescribing/Antibiotic prescribing GB 01-17 to 01-23.csv')
# df9 = pd.read_csv(
#     '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Twitter data/AMR/AMR GB 01-17 to 01-23.csv')
# df10 = pd.read_csv(
#     '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Twitter data/ABR/ABR GB 01-17 to 01-23.csv')
#
# merged_df = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, ])
#
#
# merged_df.to_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Twitter data/all_Twitter_data IE (non-English excluded).csv',
#                 index=False)


twitter_IE = pd.read_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Twitter data/all_Twitter_data IE (non-English excluded)_new.csv', dtype=str)
twitter_GB =  pd.read_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Twitter data/all_Twitter_data GB (non-English excluded)_new.csv', dtype=str)

merged_twitter = pd.concat([twitter_GB,twitter_IE])
# merged_twitter.to_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Twitter data/all_Twitter_data.csv',
#                 index=False)


all_ig = pd.read_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/all_Instagram_data(non-English excluded).csv')
num_rows = merged_twitter.shape[0]
num_cols = merged_twitter.shape[1]

print("row：", num_rows)
print("col：", num_cols)
print("row and col：", merged_twitter.shape)


print(all_ig.columns)
print(merged_twitter.columns)


# id --> ID; text -->Caption