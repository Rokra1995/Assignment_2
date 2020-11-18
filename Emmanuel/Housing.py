import pandas as pd

#data is a dataframe
data = pd.read_csv('housing_data.csv', sep=",")
print(data.head())

'''
print(data.head())
print(data[:3])
print(data.movieName.iloc[1])
'''

#Column_names is a list of strings
column_names = ['globalid','publicatiedatum','postcode','koopPrijs','soortWoning','categorieObject','bouwjaar','indTuin','perceelOppervlakte','kantoor_naam_MD5hahs','aantalKamer','aantalBadkamers','energielabelKlasse','globalid','oppervlakte','datum_ondertekening']

'''
#subset is a next dataframe that has been filtered on your favorite movie
subset = pd.Dataframe(columns = column_names)


for movie in range(100):
    if data.movieName.iloc[movie] == 'beach-rats':
    row=data[movie:movie + 1]
    print(row)
    subset.append(row)

'''
    
    
