import pandas as pd
import seaborn as sns  # for plotting
import matplotlib.pyplot as plt  # for showing plot

data = pd.read_excel('/Users/wei/Documents/cara_network/amr_igdata/output/final_602posts_data.xlsx')

print(data.describe())
print(data.columns)

# Creating a Histogram

sns.histplot(data['cat'], kde=False).set_title('Histogram of categories')
plt.show()

sns.histplot(data['cat'], kde=True).set_title('Histogram of categories')  # kde: plot density estimation
plt.show()