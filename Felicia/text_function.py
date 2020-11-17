# Create the text search function to the python wrapper

# What is the mean idea?

# look up in the booklet -> pass a word in the function and look in which description this is 

import pandas as pd

#data is a dataframe
data = pd.read_csv('/home/felies/Assignment_2/Input_data/housing_data.csv')

print(data.head())

for word in range(100):
    if 