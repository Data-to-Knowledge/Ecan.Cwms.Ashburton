# -*- coding: utf-8 -*-
"""
Created on Fri Apr 26 14:47:14 2019

@author: michaelek
"""
import os
import pandas as pd
from ecandbparams import sql_arg
from allotools import AlloUsage
from pdsql import mssql
import parameters as param
import eto_estimates as eto
from hydrolm import LM

pd.options.display.max_columns = 10

###################################
### Parameters

grp = ['wap', 'use_type']
datasets = ['allo', 'restr_allo', 'usage']

use_type_dict = {'industrial': 'other', 'municipal': 'other'}

###################################
### Read existing usage data

allo1 = AlloUsage(param.from_date, param.to_date, site_filter={'SwazGroupName': param.swaz_grps})

usage1 = allo1.get_ts(datasets, 'M', grp)

usage2 = usage1.loc[usage1.sw_allo > 0, ['sw_allo', 'sw_restr_allo', 'sw_usage']].reset_index().copy()

usage2.replace({'use_type': use_type_dict}, inplace=True)

#usage2[['sw_allo_yr', 'sw_restr_allo_yr', 'sw_usage_yr']] = usage2.groupby(['wap', 'use_type', pd.Grouper(key='date', freq='A-Jun')]).transform(sum)

grp1 = usage2.groupby('use_type')

for index, grp in grp1:
    index





































