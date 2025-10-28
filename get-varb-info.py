from data_request_api.content import dump_transformation as dt
from data_request_api.query import data_request as dr
from data_request_api.content import dreq_content as dc
from data_request_api.query import dreq_query as dq
from data_request_api.query.dreq_query import create_dreq_tables_for_variables, get_dimension_sizes
import warnings
import re
import json
import argparse
import os
import shutil
from pathlib import Path


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
        experiments = DR.get_experiments()
    except Exception as e:
        print(f"Error in accessing Data Request information: {e}")
    return DR, variables, experiments


def get_list_of_experiments(experiments):
    experiment_list = [str(exp.name) for exp in experiments]
    return experiment_list


def load_tables():
    try: 
        dreq_version    = 'v1.2.2.2'
        dreq_content    = dc.load(version=dreq_version)
        dreq_tables     = dq.create_dreq_tables_for_request(dreq_content,dreq_version=dreq_version)
        path_to_content = dc._dreq_content_loaded['json_path']
    except Exception as e:
        print(f"Error in accessing Data Request tables: {e}")
    return dreq_tables, path_to_content


def get_sampling_rate(variable):
    frequency_str = variable.get('cmip7_frequency')
    frequency_str = str(getattr(variable, 'cmip7_frequency', None))
    match         = re.search(r':\s*(.*?)\s*\(', frequency_str)

    if match:
        frequency = match.group(1)
        rate = frequency_map.get(frequency)
    else:
        rate = ''
    return rate


def get_vertical_mesh(variable):
    shape_to_vertical = {
        shape.name: getattr(shape, "vertical_mesh", None)
        for shape in dreq_tables["Spatial Shape"].records.values() }

    spatial_shape_str = str(getattr(variable, 'spatial_shape', None))
    match             = re.search(r':\s*(.*?)\s*\(', spatial_shape_str)
    if match:
        spatial_shape = match.group(1)
        vert_mesh = shape_to_vertical.get(spatial_shape)
    else:
        spatial_shape = ''
        vert_mesh = ''
    return vert_mesh, spatial_shape
    

def get_varb_data(variable):
    name            = str(getattr(variable, 'cmip6_compound_name', None))
    title           = str(getattr(variable, 'title', None))
    description     = str(getattr(variable, 'description', None))
    processing_note = str(getattr(variable, 'processing_note', None))

    if processing_note == '':
        processing_note = 'N/A'
     
    vertical_mesh, spatial_shape  = get_vertical_mesh(variable)
    sampling_rate   = get_sampling_rate(variable)
    horizontal_mesh = 68400 #360*180

    if sampling_rate != '':
        size = sampling_rate * horizontal_mesh * vertical_mesh # *4
    else: 
        size = 'sampling rate not available, cannot compute size'

    varbInfo = {'name': name, 'title': title, 'description': description, 'processing note': processing_note, 
                'spatial shape': spatial_shape, 'vertical mesh': vertical_mesh, 'horizontal mesh': horizontal_mesh, 'sampling rate': sampling_rate, 'variable size per decade': size}
    return varbInfo


def get_experiment_varbs(DR, experiment_name):
    filter = {"experiments": [experiment_name]}
    try: 
        experiment_varbs = DR.find_variables(operation='any', skip_if_missing=True, **filter)
    except Exception as e:
        print(f"Error occurred while fetching variables for experiment {experiment_name}: {e}")
        experiment_varbs = []
    return experiment_varbs


def get_varb_name_list(experiment_varbs):
    varb_names = []
    for varb in experiment_varbs:
        varb_name = varb.cmip6_compound_name.value
        varb_names.append(varb_name)
    return varb_names


def calc_tot_vol_per_exp(DR, experiment):
    experiment_varbs = get_experiment_varbs(DR, experiment)
    run_time         = experiment.get('size_(years,_minimum)')
    varb_names       = get_varb_name_list(experiment_varbs)

    all_varb_sizes_per_exp = []
    for variable in varb_names:
        for item in varbInfoList:
            if item['name'] == str(variable):
                varb_size_for_exp = item['variable size per decade'] * run_time / 10
                # print(f"experiment: {experiment.name} // variable: {variable} // variable from dict: {item['name']} // size: {item['variable size per decade']} // runtime: {run_time} // size for varible per exp: {varb_size_for_exp}")
                all_varb_sizes_per_exp.append(varb_size_for_exp)

    total_vol_size_per_exp = sum(all_varb_sizes_per_exp)
    print('Total volume size for ' + str(experiment.name) + ': ' + str(total_vol_size_per_exp))

    return total_vol_size_per_exp


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

def clean_up_dirctory():
    cache_dir = Path.home() / ".CMIP7_data_request_api_cache"
    if cache_dir.exists():
        shutil.rmtree(cache_dir)

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

varbInfo     = {}
expInfo      = {}
varbInfoList = []
expInfoList  = []
all_volumes  = []

DR, variables, experiments   = get_dr_info()
dreq_tables, path_to_content = load_tables()
experiment_list              = get_list_of_experiments(experiments)

for variable in variables:
    varbInfo = get_varb_data(variable)
    varbInfoList.append(varbInfo)

if output_option == 'all':
    write_all_to_file(varbInfoList)
if output_option == 'each':
    write_each_to_file(varbInfoList)

for experiment in experiments:
    total_vol_size_per_exp = calc_tot_vol_per_exp(DR, experiment)
    all_volumes.append(total_vol_size_per_exp)
total_vol_est = sum(all_volumes)
print(f"total volume estimate for all experiments: {total_vol_est}")

clean_up_dirctory()