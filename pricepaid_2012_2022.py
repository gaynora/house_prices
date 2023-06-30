# -*- coding: utf-8 -*-
"""
Analysing HMLR domestic property sale £ prices using the HMLR 'Price Paid' dataset
Identifying change between 2012 - 2022 for individual properties

Python3 script

@author: @gaynora
"""
import pandas

#import price paid 2012 with headers
colnames=['pricepaid', 'postcode', 'proptype', 'tenure', 'PAON', 'SAON'] 
pp_2012_pt1 = pandas.read_csv('pp-2012-part1.csv', usecols=[1,3,4,6,7,8], names=colnames )
pp_2012_pt2 = pandas.read_csv('pp-2012-part2.csv', usecols=[1,3,4,6,7,8], names=colnames )
pp_2022 = pandas.read_csv('pp-2022.csv', usecols=[1,3,4,6,7,8], names=colnames )

# also API directly

#append 2012 data together - row-wise
pp_2012 = pp_2012_pt1.append(pp_2012_pt2, ignore_index=True)

# new concatenated field to act as unique ID
pp_2012['concat'] = pp_2012['postcode'] + pp_2012['PAON'] + pp_2012['SAON'].astype(str)
pp_2022['concat'] = pp_2022['postcode'] + pp_2022['PAON'] + pp_2022['SAON'].astype(str)

# inner join 2012 and 2022 on concat field

# on inner join file calc new column % increase £ price paid
# calc av % increase price paid by type
# calc av increase price paid by tenure

# coordinates from ONSPD match
# spatial join to rural-urban calc column urban-rural
# calc av % increase price paid by urban - rural
# export individual property sale records (inner joined) to csv

# are the results statitsically significant - is the sample size against all domestic properties in E & W big enough?
# 24,782,800 households England and Wales Census 21 March 2021
