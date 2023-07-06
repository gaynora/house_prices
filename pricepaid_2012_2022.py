# -*- coding: utf-8 -*-
"""
Analysing HMLR domestic property sale £ prices using the HMLR 'Price Paid' dataset
Identifying change between 2012 - 2022 for individual properties

Python3 script

@author: @gaynora
"""
import pandas

#import price paid 2012 with headers and specified datatypes. Data source: https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
colnames=['pricepaid', 'date','postcode', 'proptype', 'tenure', 'PAON', 'SAON', 'street']
type_dict = {'pricepaid': 'int', 'date': 'str', 'postcode': 'str', 'proptype': 'str', 'tenure': 'str', 'PAON':'str', 'SAON':'str', 'street': 'str'}
pp_2012_pt1 = pandas.read_csv('pp-2012-part1.csv', usecols=[1,2,3,4,6,7,8,9], names=colnames, dtype=type_dict )
pp_2012_pt2 = pandas.read_csv('pp-2012-part2.csv', usecols=[1,2,3,4,6,7,8,9], names=colnames )
pp_2022 = pandas.read_csv('pp-2022.csv', usecols=[1,2,3,4,6,7,8,9], names=colnames )
pp_2012_pt1.date = pandas.to_datetime(pp_2012_pt1['date'])# convert dates to datetime data type 
pp_2012_pt2.date = pandas.to_datetime(pp_2012_pt2['date'])
pp_2022.date = pandas.to_datetime(pp_2022['date'])
# csv but also play around with API directly

#append 2012 data together - row-wise
pp_2012 = pp_2012_pt1.append(pp_2012_pt2, ignore_index=True)
pp_2012_pt1.drop(pp_2012_pt1.index , inplace=True)
pp_2012_pt2.drop(pp_2012_pt2.index , inplace=True)

# new concatenated field to act as unique ID
pp_2012['concat2012'] = pp_2012['postcode'] + pp_2012['PAON'] + pp_2012['SAON'].astype(str) + pp_2012['street']
pp_2022['concat2022'] = pp_2022['postcode'] + pp_2022['PAON'] + pp_2022['SAON'].astype(str) + pp_2022['street']
pp_2012.drop(['PAON', 'SAON', 'street'], axis=1, inplace=True) #no longer needed
pp_2022.drop(['PAON', 'SAON', 'street'], axis=1, inplace=True) #no longer needed

#identify recs sold multiple times in the year and delete before joining  
duplicates_2012 = pp_2012[pp_2012.duplicated(['concat2012'], keep=False)]
duplicates_2022 = pp_2022[pp_2022.duplicated(['concat2022'], keep=False)]
#identify records to delete from main table
#2012 we want to keep the earliest records - so these are a list of the latest records
delete_2012 = duplicates_2012.sort_values('date').drop_duplicates('concat2012',keep='first') #drop the duplicate records with the earliest date(s) / keeps the latest record
#2022 we want to keep the latest records - so these are a list of the earliest records
delete_2022 = duplicates_2022.sort_values('date').drop_duplicates('concat2022',keep='last') #drop the duplicate records with the latest date(s) / keeps the earliest record
#delete duplicate recs from the main dataframes
dups_removed_2012 = 
dups_removed_2022 = 

dups_removed_2012.drop(['date'], axis=1, inplace=True) #no longer needed
dups_removed_2022.drop(['date'], axis=1, inplace=True) #no longer needed

# records with null concat field need deleting before joining
dups_removed_2012.drop(dups_removed_2012[dups_removed_2012['concat2012'].isnull()].index, inplace = True) # 658,717 recs remaining
dups_removed_2022.drop(dups_removed_2022[dups_removed_2022['concat2022'].isnull()].index, inplace = True)


#join 2012 and 2022 on concat field
joined = dups_removed_2012.merge(dups_removed_2022, left_on='concat2012', right_on='concat2022', how='inner') #30,301 recs = 0.12% sample - 24,782,800 households England and Wales Census 21 March 2021

# calculate new column % increase £ price paid
joined['perchange'] = (joined['pricepaid_y'] - joined['pricepaid_x']) / joined['pricepaid_y']
# calc av % increase price paid by type - aggregated cross segmentaion
# calc av increase price paid by tenure - aggregated cross segmentation

#geocode to perform spatial analysis
onspd = pandas.read_csv('ONSPD_MAY_2023_UK.csv', usecols=[2,11,12, 40], header=0 ) # data source: https://geoportal.statistics.gov.uk/datasets/ons-postcode-directory-may-2023/about
geocoded = joined.merge(onspd, left_on='postcode_y', right_on='pcds', how='left')
geocoded.to_csv('geocoded.csv')
# calc av % increase price paid by urban - rural
# calc av % increase price paid by tenure
# calc av % increase price paid by property type
# calc av % increase price paid multivariate segmented by prop type and urban/rural 
# seaborn?
# when cross-segmented, are the record counts  > 385 for statistical significance (or other population size i.e. number of flats)
