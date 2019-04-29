# -*- coding: utf-8 -*-
"""
Created on Mon Apr 29 12:20:30 2019

@author: michaelek
"""
import os
import pandas as pd
from pdsql import mssql
import usage_estimates as ue
import upstream_takes as takes
import parameters as param

pd.options.display.max_columns = 10

#######################################
### Parameters











#####################################
### Naturalisation














































############################################
### Testing

### Resample SD to daily time series
days2 = pd.to_timedelta((days1/2).round().astype('int32'), unit='D')
sd3 = sd2.drop('sd_usage', axis=1)
sd3.loc[:, 'time'] = sd3.loc[:, 'time'] - days2
grp1 = sd3.groupby(['site'])
first1 = grp1.first()
last1 = sd2.groupby('site')[['time', 'sd_rate']].last()
first1.loc[:, 'time'] = pd.to_datetime(first1.loc[:, 'time'].dt.strftime('%Y-%m') + '-01')
sd4 = pd.concat([first1.reset_index(), sd3, last1.reset_index()]).reset_index(drop=True).sort_values(['site', 'time'])
sd5 = sd4.set_index('time')
sd6 = sd5.groupby('site').apply(lambda x: x.resample('D').interpolate(method='pchip'))['sd_rate']

### Naturalise flows
nat1 = pd.concat([flow, sd6], axis=1, join='inner')
nat1['nat_flow'] = nat1['flow'] + nat1['sd_rate']

