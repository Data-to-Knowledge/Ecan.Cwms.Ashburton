# -*- coding: utf-8 -*-
"""
Created on Mon Apr 29 12:20:30 2019

@author: michaelek
"""
import os
import pandas as pd
import usage_estimates as ue
import upstream_takes as takes
import flow_estimates as flow
import parameters as param

pd.options.display.max_columns = 10

#######################################
### Parameters

flow_site_sw_usage_csv = 'flow_site_sw_usage_rate_{}.csv'.format(param.run_time)
nat_flow_csv = 'nat_flow_{}.csv'.format(param.run_time)

#####################################
### Naturalisation

print('Naturalise the flow data')

#try:
#    site_rate = pd.read_csv(os.path.join(param.results_path, flow_site_sw_usage_csv), parse_dates=['date'], infer_datetime_format=True)
#
#except:
## Process usage data

usage_rate2 = ue.usage_daily_rate.copy()

## Combine usage with site data

print('-> Combine usage with site data')

waps1 = takes.waps_gdf.drop(['geometry', 'SwazGroupName', 'SwazName'], axis=1).copy()

usage_rate3 = pd.merge(waps1, usage_rate2.reset_index(), on='wap')

site_rate = usage_rate3.groupby(['flow_site', 'date'])[['sw_usage_rate']].sum().reset_index()

site_rate.to_csv(os.path.join(param.results_path, flow_site_sw_usage_csv), index=False)

## Add usage to flow
print('-> Add usage to flow')

flow1 = flow.flow.stack().reset_index()
flow1.columns = ['date', 'flow_site', 'flow']

flow2 = pd.merge(flow1, site_rate, on=['flow_site', 'date'], how='left').set_index(['flow_site', 'date']).sort_index()
flow2.loc[flow2.sw_usage_rate.isnull(), 'sw_usage_rate'] = 0

flow2['nat flow'] = flow2['flow'] + flow2['sw_usage_rate']

nat_flow = flow2.copy()

nat_flow.to_csv(os.path.join(param.results_path, nat_flow_csv))



############################################
### Testing


