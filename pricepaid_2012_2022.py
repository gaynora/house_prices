# -*- coding: utf-8 -*-
"""
Analysing HMLR domestic property sale £ prices using the HMLR 'Price Paid' dataset
Identifying change between 2012 - 2022 for individual properties
and examining against urban-rural classification, regional patterns, and property type
checking counts for statistical significance
all data is open government data under OGL licence

Python3 script

@author: @gaynora
"""
import pandas
import geopandas
import geoplot as gplt
import geoplot.crs as gcrs
import mapclassify as mc

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
pp_2012 = pandas.concat([pp_2012_pt1, pp_2012_pt2], ignore_index=True)
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
#identify records to actually delete from main table
#2012 we want to keep the earliest records - so these are a list of the latest records to delete
todelete_2012 = duplicates_2012.sort_values('date').drop_duplicates('concat2012',keep='first') #drop the duplicate records with the earliest date(s) / keeps the latest record
#2022 we want to keep the latest records - so these are a list of the earliest records to delete
todelete_2022 = duplicates_2022.sort_values('date').drop_duplicates('concat2022',keep='last') #drop the duplicate records with the latest date(s) / keeps the earliest record
#delete duplicate recs from the main dataframes
cond = pp_2012['concat2012'].isin(todelete_2012['concat2012']) # first df is the records to keep, second df contains the records to delete
pp_2012.drop(pp_2012[cond].index, inplace = True)
cond = pp_2022['concat2022'].isin(todelete_2022['concat2022']) # first df is the records to keep, second df contains the records to delete
pp_2022.drop(pp_2022[cond].index, inplace = True)

pp_2012.drop(['date'], axis=1, inplace=True) #no longer needed
pp_2022.drop(['date'], axis=1, inplace=True) #no longer needed

# records with null concat field also need deleting before joining
pp_2012.drop(pp_2012[pp_2012['concat2012'].isnull()].index, inplace = True) # 658,717 recs remaining
pp_2022.drop(pp_2022[pp_2022['concat2022'].isnull()].index, inplace = True)

#join 2012 and 2022 on concat field
joined = pp_2012.merge(pp_2022, left_on='concat2012', right_on='concat2022', how='inner') #30,301 recs = 0.12% sample - 24,782,800 households England and Wales Census 21 March 2021

# calculate new column % increase £ price paid
joined['perchange'] = (joined['pricepaid_y'] - joined['pricepaid_x']) / joined['pricepaid_y'] *100

#geocode to perform spatial analysis
onspd = pandas.read_csv('ONSPD_MAY_2023_UK.csv', usecols=[2,11,12, 17,40, 42,43], header=0 ) # data source: https://geoportal.statistics.gov.uk/datasets/ons-postcode-directory-may-2023/about
geocoded = joined.merge(onspd, left_on='postcode_y', right_on='pcds', how='left')
#add additional descriptions to match codes
urban_rural_class_dict = {"A1": "Major Conurbation", "B1": "Minor Conurbation", "C1": "City and Town", "C2": "City and Town in a Sparse Setting", "D1": "Town and Fringe", "D2":"Town and Fringe in a Sparse Setting", "E1": "Village", "E2": "Village in a Sparse Setting", "F1": "Hamlets and Isolated Dwellings", "F2": "Hamlets and Isolated Dwellings in a Sparse Setting"}
geocoded['urbrur11'] = geocoded['ru11ind'].map(urban_rural_class_dict)
urban_rural_class_dictb = {"A1": "Urban", "B1": "Urban", "C1": "Urban", "C2": "Urban", "D1": "Rural", "D2":"Rural", "E1": "Rural", "E2": "Rural", "F1": "Rural", "F2": "Rural"}
geocoded['urbrur11b'] = geocoded['ru11ind'].map(urban_rural_class_dictb)
prop_type_dict = {"T": "Terraced", "S": "Semi-Detached", "F": "Flat/Maisonette", "D": "Detatched", "O": "Other"}
geocoded['typedesc'] = geocoded['proptype_x'].map(prop_type_dict)
region_dict = {"W99999999": "Wales", "E12000001": "North East", "E12000002": "North West", "E12000003": "Yorkshire and The Humber", "E12000004": "East Midlands", "E12000005": "West Midlands", "E12000006": "East of England", "E12000007": "London", "E12000008": "South East", "E12000009": "South West"}
geocoded['region'] = geocoded['rgn'].map(region_dict)

#create geodataframe from xy coordinate fields
gdf = geopandas.GeoDataFrame(geocoded, geometry=geopandas.points_from_xy(geocoded.long,geocoded.lat)) 
gdf = gdf.set_crs(4326, allow_override=True)

#export all records - can be used elsewhere, particularly for web mapping if needed
#geocoded.to_csv('price_paid_2012_2022.csv')
#gdf.to_file('price_paid_2012_2022.geojson', driver='geojson')

# remove records of property type 'Other' - the group is both unhelpful and counts are too small to be statistically significant
geocoded.drop(geocoded[geocoded.typedesc == 'Other'].index, inplace=True)

# plot average % increase by:

# grouped by both but only using main urban / rural marker , because too many counts are too small / statistically insignificant using the full urban - rural segmentation
#geocoded.groupby(['urbrur11b', 'typedesc'])['perchange'].mean().plot(kind='barh', color = '#f78da7')
#plt.xlabel("% change price paid 2012-2022") # all of labels and title need changing if data changes
#plt.ylabel("Urban Rural Classification by property type")
#plt.title("Average property value uplift 2012-2022 by Urban Rural Classification and property type in England and Wales")

# grouped by by property type
#geocoded.groupby(['typedesc'])['perchange'].mean().reindex(['Flat/Maisonette', 'Terraced', 'Semi-Detached', 'Detatched']).plot(kind='barh', color = '#f78da7')
#plt.xlabel("% change price paid 2012-2022") # all of labels and title need changing if data changes
#plt.ylabel("Property type")
#plt.title("Average property value uplift 2012-2022 by property type in England and Wales")

# grouped by full Urban Rural Classification
# not showing all groups 'in a sparse setting' as counts are too small / statistically insignificant
#geocoded.groupby(['urbrur11'])['perchange'].mean().reindex([ 'Hamlets and Isolated Dwellings', 'Village','Town and Fringe',  'City and Town', 'Minor Conurbation', 'Major Conurbation']).plot(kind='barh', color = '#abb8c3')
#plt.xlabel("% change price paid 2012-2022") # all of labels and title need changing if data changes
#plt.ylabel("Urban Rural Classification")
#plt.title("Average property value uplift 2012-2022 by Urban Rural Classification in England and Wales")

# grouped by region and property type
#geocoded.groupby(['region', 'typedesc'])['perchange'].mean().plot(kind='barh', color = '#f78da7')
#plt.xlabel("% change price paid 2012-2022") # all of labels and title need changing if data changes
#plt.ylabel("Region by property type")
#plt.title("Average property value uplift 2012-2022 by Region and property type in England and Wales")

# grouped by region
#geocoded.groupby(['region'])['perchange'].mean().plot(kind='barh', color = '#f78da7')
#plt.xlabel("% change price paid 2012-2022") # all of labels and title need changing if data changes
#plt.ylabel("Region by region")
#plt.title("Average property value uplift 2012-2022 by region in England and Wales")

# Examine basic descriptive statistics of the percentage £ change
print('minimum % change: ', geocoded['perchange'].min())
print('maximum % change: ', geocoded['perchange'].max())
print('mean % change: ', geocoded['perchange'].mean())

# PLOT OVERALL FREQUENCY DISTRIBUTION OF % CHANGE
# geoplot method - expects gdf input to be in lat-long

scheme = mc.Quantiles(gdf['perchange'], k=5)
extent = gdf.total_bounds
ax = gplt.pointplot(
    gdf,  
    projection=gcrs.Mercator(),
    extent=extent,
    hue='perchange', 
    cmap='inferno_r',
    scheme=scheme,
    legend=True, 
    legend_var='hue',
    figsize=(20,40),
    legend_kwargs={'frameon': True, 'markersize':50, 'fontsize':34}
)
ax.set_title('% change price 2012-22 by property, England & Wales', fontsize=34)


# return and export data tables, including counts to check statistical significance
groupby_proptype = geocoded.groupby(['urbrur11'])['perchange'].mean()
groupby_proptype.to_csv('perchange_by_urban_rural_mean.csv')
groupby_urbanrural = geocoded.groupby(['typedesc'])['perchange'].mean()
groupby_urbanrural.to_csv('perchange_by_proptype_mean.csv')
groupby_urbanrural_pt_ = geocoded.groupby(['typedesc', 'urbrur11b'])['perchange'].mean()
groupby_urbanrural_pt_.to_csv('perchange_by_proptype_urban_rural_mean.csv')

groupby_region_proptype = geocoded.groupby(['region', 'typedesc'])['perchange'].mean()
groupby_region_proptype.to_csv('perchange_by_region_proptype_mean.csv')

groupby_region = geocoded.groupby(['region'])['perchange'].mean()
groupby_region.to_csv('perchange_by_region_mean.csv')

groupby_proptypeb = geocoded.groupby(['urbrur11'])['perchange'].count()
groupby_proptypeb.to_csv('perchange_by_urban_rural_counts.csv')
groupby_urbanruralb = geocoded.groupby(['typedesc'])['perchange'].count()
groupby_urbanruralb.to_csv('perchange_by_proptype_counts.csv')
groupby_urbanrural_ptb = geocoded.groupby(['typedesc', 'urbrur11'])['perchange'].count()
groupby_urbanrural_ptb.to_csv('perchange_by_proptype_urban_ruralall_counts.csv')
groupby_urbanrural_ptc = geocoded.groupby(['typedesc', 'urbrur11b'])['perchange'].count()
groupby_urbanrural_ptc.to_csv('perchange_by_proptype_urban_rural_counts.csv')

groupby_region_proptypeb = geocoded.groupby(['region', 'typedesc'])['perchange'].count()
groupby_region_proptypeb.to_csv('perchange_by_region_proptype_counts.csv')

groupby_regionb = geocoded.groupby(['region'])['perchange'].count()
