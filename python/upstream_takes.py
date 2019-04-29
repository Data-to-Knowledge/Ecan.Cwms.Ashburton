# -*- coding: utf-8 -*-
"""
Created on Wed Apr 24 09:32:35 2019

@author: michaelek
"""
import os
import pandas as pd
import geopandas as gpd
from gistools import rec, vector
from ecandbparams import sql_arg
from allotools import AlloUsage
from pdsql import mssql
import parameters as param

pd.options.display.max_columns = 10

#######################################
### Parameters

site_csv = 'ashburton_sites.csv'

server = param.hydro_server
database = 'hydro'
sites_table = 'ExternalSite'
crc_wap_table = 'CrcWapAllo'

rec_rivers_sql = 'rec_rivers_gis'
rec_catch_sql = 'rec_catch_gis'

from_date = '2019-04-01'
to_date = '2019-06-30'

catch_del_shp = 'catch_del_{}.shp'.format(param.run_time)
allo_csv = 'allo_{}.csv'.format(param.run_time)
allo_wap_csv = 'allo_wap_{}.csv'.format(param.run_time)
waps_shp = 'waps_{}.shp'.format(param.run_time)
min_flow_sites_shp = 'min_flow_sites_{}.shp'.format(param.run_time)

######################################
### Read in data
print('Read in allocation and sites data')

try:
    min_flow_sites_gdf = gpd.read_file(os.path.join(param.results_path, min_flow_sites_shp))
    catch_gdf = gpd.read_file(os.path.join(param.results_path, catch_del_shp))
    waps_gdf = gpd.read_file(os.path.join(param.results_path, waps_shp))
    allo_wap = pd.read_csv(os.path.join(param.results_path, allo_wap_csv))
    allo = pd.read_csv(os.path.join(param.results_path, allo_csv))

    print('-> loaded from local files')

except:
    print('-> Processing data from the databases')

    sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'NZTMX', 'NZTMY', 'SwazGroupName', 'SwazName'])

    ash_sites1 = pd.read_csv(os.path.join(param.inputs_path, site_csv)).site.astype(str)

    sites0 = sites1[sites1.ExtSiteID.isin(ash_sites1)].copy()
    sites0.rename(columns={'ExtSiteID': 'min_flow_site'}, inplace=True)

    min_flow_sites_gdf = vector.xy_to_gpd('min_flow_site', 'NZTMX', 'NZTMY', sites0)
    min_flow_sites_gdf.to_file(os.path.join(param.results_path, min_flow_sites_shp))

    sql1 = sql_arg()

    rec_rivers_dict = sql1.get_dict(rec_rivers_sql)
    rec_catch_dict = sql1.get_dict(rec_catch_sql)

    rec_rivers = mssql.rd_sql(**rec_rivers_dict)
    rec_catch = mssql.rd_sql(**rec_catch_dict)

    ###################################
    ### Catchment delineation and WAPs

    catch_gdf = rec.catch_delineate(min_flow_sites_gdf, rec_rivers, rec_catch)
    catch_gdf.to_file(os.path.join(param.results_path, catch_del_shp))

    wap1 = mssql.rd_sql(server, database, crc_wap_table, ['wap']).wap.unique()

    sites3 = sites1[sites1.ExtSiteID.isin(wap1)].copy()
    sites3.rename(columns={'ExtSiteID': 'wap'}, inplace=True)

    sites4 = vector.xy_to_gpd('wap', 'NZTMX', 'NZTMY', sites3)
    sites4 = sites4.merge(sites3.drop(['NZTMX', 'NZTMY'], axis=1), on='wap')

    waps_gpd, poly1 = vector.pts_poly_join(sites4, catch_gdf, 'min_flow_site')
    waps_gpd.to_file(os.path.join(param.results_path, waps_shp))

    ##################################
    ### Get crc data

    allo1 = AlloUsage(crc_wap_filter={'wap': waps_gpd.wap.unique().tolist()}, from_date=from_date, to_date=to_date)

    #allo1.allo[allo1.allo.crc_status == 'Terminated - Replaced']

    allo_wap1 = allo1.allo_wap.copy()
    allo_wap = pd.merge(allo_wap1.reset_index(), waps_gpd.drop('geometry', axis=1), on='wap')

    allo = allo1.allo.copy()

    allo_wap.to_csv(os.path.join(param.results_path, allo_wap_csv))
    allo.to_csv(os.path.join(param.results_path, allo_csv))


#################################
### Testing

#nzreach1 = 13053151
#
#c1 = rec_catch[rec_catch.NZREACH == nzreach1]
#r1 = rec_rivers[rec_rivers.NZREACH == nzreach1]
#r2 = rec_streams[rec_streams.NZREACH == nzreach1]























