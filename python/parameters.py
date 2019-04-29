# -*- coding: utf-8 -*-
"""
Created on Tue Apr 24 09:29:52 2018

@author: michaelek
"""
import os
from configparser import ConfigParser
import pandas as pd

run_time = pd.Timestamp.today().strftime('%Y%m%d')

#####################################
### Parameters

## Paths
py_path = os.path.realpath(os.path.dirname(__file__))
#project_path = os.path.split(py_path)[0]

results_dir = 'results'
inputs_dir = 'inputs'

ini1 = ConfigParser()
ini1.read([os.path.join(py_path, os.path.splitext(__file__)[0] + '.ini')])

## Input
#niwa_climate_url = str(ini1.get('Input', 'niwa_climate_url'))

hydro_server = str(ini1.get('Input', 'server'))
hydro_database = 'Hydro'

project_path = str(ini1.get('Input', 'project_path'))

results_path = os.path.join(project_path, results_dir)
inputs_path = os.path.join(project_path, inputs_dir)

if not os.path.exists(results_path):
    os.makedirs(results_path)

if not os.path.exists(inputs_path):
    os.makedirs(inputs_path)

## Other

#rerun_usage_estimates = bool(int(ini1.get('Input', 'rerun_usage_estimates')))

#swaz_grps = ['Pendarves', 'Ashburton River', 'Hinds']

from_date = str(ini1.get('Input', 'from_date'))
to_date = str(ini1.get('Input', 'to_date'))

min_gaugings = int(ini1.get('Input', 'min_gaugings'))
buffer_dis = int(ini1.get('Input', 'buffer_dis'))


