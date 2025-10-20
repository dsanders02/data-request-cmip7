from data_request_api.content import dump_transformation as dt
from data_request_api.query import data_request as dr
import warnings
import pprint
import re
import json
import argparse
import os

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
        content_dic = dt.get_transformed_content(version='v1.2.2.2')
        DR          = dr.DataRequest.from_separated_inputs(**content_dic)
        variables   = DR.get_variables()
    except Exception as e:
        variables = ''
        print(f"Error in accessing Data Request information: {e}")
    return DR, variables

def get_sampling_rate(variable):
    frequency_str = str(getattr(variable, 'cmip7_frequency', None))
    match = re.search(r':\s*(.*?)\s*\(', frequency_str)
    if match:
        frequency = match.group(1)
        rate = frequency_map.get(frequency)
    else:
        rate = ''
    return rate

def get_varb_data(variable):
    name            = str(getattr(variable, 'cmip6_compound_name', None))
    title           = str(getattr(variable, 'title', None))
    description     = str(getattr(variable, 'description', None))
    processing_note = str(getattr(variable, 'processing_note', None))
    
    if processing_note == '':
        processing_note = 'N/A'

    sampling_rate   = get_sampling_rate(variable)
    horizontal_mesh = 68400
    if sampling_rate != '':
        size = sampling_rate * horizontal_mesh * 4
    else: 
        size = 'sampling rate not available, cannot compute size'

    varbInfo = {'name': name, 'title': title, 'description': description, 'processing note': processing_note, 'size': size }
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


## MAIN ##
parser = argparse.ArgumentParser(description='''This script retrieves variable information from the data request API.''')
parser.add_argument('--all',   '-a', help='Write output to a single file'   , action='store_true')
parser.add_argument('--each',  '-e', help='Write output to individual files', action='store_true')
# parser.add_argument('--verbose','-v', help='turn on debug messages', action='store_true')
args = parser.parse_args()
if args.all:
    output_option = 'all'
if args.each:
    output_option = 'each'

DR, variables = get_dr_info()

varbInfoList = []
varbInfo     = {}

if variables != '':
    for variable in variables:
        varbInfo = get_varb_data(variable)
        varbInfoList.append(varbInfo)
    if output_option == 'all':
        write_all_to_file(varbInfoList)
    if output_option == 'each':
        write_each_to_file(varbInfoList)

if variables == '':
    print("Could not find variable information, check API connection.")