# -*- coding: utf-8 -*-
"""
Created on Fri Apr 26 14:47:14 2019

@author: michaelek
"""
import os
import pandas as pd
from allotools import AlloUsage
from pdsql import mssql
import parameters as param

pd.options.display.max_columns = 10

###################################
### Parameters

grp = ['wap', 'use_type']
datasets = ['allo', 'restr_allo', 'usage']

use_type_dict = {'industrial': 'other', 'municipal': 'other'}

server = param.hydro_server
database = 'hydro'
sites_table = 'ExternalSite'

swaz_mon_ratio_csv = 'swaz_mon_ratio_{}.csv'.format(param.run_time)
allo_usage_wap_swaz_csv = 'allo_usage_wap_swaz_{}.csv'.format(param.run_time)
wap_sw_usage_csv = 'wap_sw_usage_rate_{}.csv'.format(param.run_time)

###################################
### Read existing usage data
print('Read in usage estimates')

try:
    res_swaz5 = pd.read_csv(os.path.join(param.results_path, swaz_mon_ratio_csv))
    usage4 = pd.read_csv(os.path.join(param.results_path, allo_usage_wap_swaz_csv), parse_dates=['date'], infer_datetime_format=True)
    usage_rate = pd.read_csv(os.path.join(param.results_path, wap_sw_usage_csv), parse_dates=['date'], infer_datetime_format=True)

    print('-> loaded from local files')

except:

    print('-> Processing usage data from the databases')

    allo1 = AlloUsage(param.from_date, param.to_date, site_filter={'SwazGroupName': param.swaz_grps})

    usage1 = allo1.get_ts(datasets, 'M', grp)

    usage2 = usage1.loc[usage1.sw_restr_allo > 0, ['sw_restr_allo', 'sw_usage']].reset_index().copy()

    usage2.replace({'use_type': use_type_dict}, inplace=True)

    usage2[['sw_restr_allo_yr', 'sw_usage_yr']] = usage2.groupby(['wap', 'use_type', pd.Grouper(key='date', freq='A-JUN')]).transform('sum')

    sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'SwazGroupName', 'SwazName'], where_in={'ExtSiteID': usage2.wap.unique().tolist()})
    sites1.rename(columns={'ExtSiteID': 'wap'}, inplace=True)

    usage0 = pd.merge(sites1, usage2, on='wap')
    usage0['mon'] = usage0.date.dt.month

    usage0['mon_ratio'] = usage0.sw_usage/usage0.sw_restr_allo
    usage0['yr_ratio'] = usage0.sw_usage_yr/usage0.sw_restr_allo_yr

    usage0.set_index(['wap', 'date'], inplace=True)

    filter1 = (usage0['yr_ratio'] >= 0.04) & (usage0['yr_ratio'] <= 2) & (usage0['mon_ratio'] >= 0.001)

    usage3 = usage0[filter1].reset_index().copy()

    res_swaz1 = usage3.groupby(['SwazGroupName', 'SwazName', 'use_type', 'mon']).mon_ratio.mean()
    res_grp1 = usage3.groupby(['SwazGroupName', 'use_type', 'mon']).mon_ratio.mean()
    res_grp1.name = 'grp_ratio'

    res_grp2 = usage3.groupby(['use_type', 'mon']).mon_ratio.mean()
    res_grp2.name = 'gross_ratio'

    all1 = usage0.groupby(['SwazGroupName', 'SwazName', 'use_type', 'mon']).mon.first()

    res_swaz2 = pd.concat([res_swaz1, all1], axis=1).drop('mon', axis=1)
    res_swaz3 = pd.merge(res_swaz2.reset_index(), res_grp1.reset_index(), on=['SwazGroupName', 'use_type', 'mon'], how='left')
    res_swaz4 = pd.merge(res_swaz3, res_grp2.reset_index(), on=['use_type', 'mon'], how='left')

    res_swaz4.loc[res_swaz4.mon_ratio.isnull(), 'mon_ratio'] = res_swaz4.loc[res_swaz4.mon_ratio.isnull(), 'grp_ratio']

    res_swaz4.loc[res_swaz4.mon_ratio.isnull(), 'mon_ratio'] = res_swaz4.loc[res_swaz4.mon_ratio.isnull(), 'gross_ratio']

    res_swaz5 = res_swaz4.drop(['grp_ratio', 'gross_ratio'], axis=1).copy()

    ### Estimate daily usage by WAP

    usage4 = pd.merge(usage0.drop(['mon_ratio', 'yr_ratio', 'sw_restr_allo_yr', 'sw_usage_yr'], axis=1).reset_index(), res_swaz5, on=['SwazGroupName', 'SwazName', 'use_type', 'mon'], how='left').set_index(['wap', 'date'])

    usage4.loc[~filter1, 'sw_usage'] = usage4.loc[~filter1, 'sw_restr_allo'] * usage4.loc[~filter1, 'mon_ratio']

    usage_rate = usage4[['sw_usage']].reset_index().copy()
    usage_rate.rename(columns={'sw_usage': 'sw_usage_rate'}, inplace=True)
    days1 = usage_rate.date.dt.daysinmonth
    usage_rate['sw_usage_rate'] = usage_rate['sw_usage_rate'] / days1 /24/60/60

    usage4.reset_index(inplace=True)

    ### Save results

    res_swaz5.to_csv(os.path.join(param.results_path, swaz_mon_ratio_csv), index=False)
    usage4.to_csv(os.path.join(param.results_path, allo_usage_wap_swaz_csv), index=False)
    usage_rate.to_csv(os.path.join(param.results_path, wap_sw_usage_csv), index=False)











############################
### Testing


























