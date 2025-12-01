from data_request_api.content import dump_transformation as dt
from data_request_api.query import data_request as dr
from data_request_api.content import dreq_content as dc
from data_request_api.query import dreq_query as dq
from data_request_api.query.dreq_query import create_dreq_tables_for_variables
import pandas as pd
import numpy as np
import warnings
import re
import json
import argparse
import os
import shutil
from pathlib import Path

## This script pulls data from the data request API version v1.2.2.2. It retrieves variable information including
## variable name, title, description and processing note. There are two options when runninig this script. Firstly,
## is the option to write all the information for the variables to a single file "variable-info-output.json". Secondly, is 
## the option to write the individual variable information to separate files, located in "variable-data/<variable name>.json"

warnings.simplefilter("ignore", UserWarning)

frequency_map = {'1hr': 87600, 
                 '3hr': 29200,
                 '6hr': 14600,
                 'day': 3650,
                 'dec': 1,
                 'fx': 1,    ## fixed -- could be something else?                     
                 'mon': 120,
                 'subhr': 262800,
                 'yr': 10 }


def get_dr_info():
    try: 
        content_dic   = dt.get_transformed_content(version='v1.2.2.2')
        DR            = dr.DataRequest.from_separated_inputs(**content_dic)
        variables     = DR.get_variables()
    except Exception as e:
        print(f"Error in accessing Data Request information: {e}")
    return DR, variables


def get_varb_data(variable):
    name            = str(getattr(variable, 'cmip6_compound_name', None))
    title           = str(getattr(variable, 'title', None))
    description     = str(getattr(variable, 'description', None))
    processing_note = str(getattr(variable, 'processing_note', None))

    if processing_note == '':
        processing_note = 'N/A'
     
    varbInfo = {'name': name, 'title': title, 'description': description, 'processing note': processing_note}

    return varbInfo


def write_all_to_file(data):
    file_path = "variable-info-output.json"
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)
    print(str(len(varbInfoList)) + " variables processed. Variable information can be found in 'variable-info-output.json'")


def write_each_to_file(data):
    for values in data:
        varb_name = values['name']
        path = 'variable-data/'
        file_name = varb_name + '.json'
        file_path = os.path.join(path, file_name)
        os.makedirs(path, exist_ok=True)
        with open(file_path, 'w') as f:
            json.dump(values, f, indent=4)
    print(str(len(varbInfoList)) + " variables processed. Files located in directroy 'variable-data'")


#### MAIN ####
parser = argparse.ArgumentParser(description='''This script retrieves variable information from the data request API and writes to json file(s).''')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--all',   '-a', help='Write output to a single file'   , action='store_true')
group.add_argument('--each',  '-e', help='Write output to individual files', action='store_true')
args = parser.parse_args()
if args.all:
    output_option = 'all'
if args.each:
    output_option = 'each'

DR, variables = get_dr_info()

varbInfo     = {}
varbInfoList = []

for variable in variables:
    varbInfo = get_varb_data(variable)
    varbInfoList.append(varbInfo)

if output_option == 'all':
    write_all_to_file(varbInfoList)
if output_option == 'each':
    write_each_to_file(varbInfoList)