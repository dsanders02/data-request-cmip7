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
import shutil
from pathlib import Path

## This script pulls data from the data request API version v1.2.2.2 to estimate the total volume of data to be pulled from the CMIP7 Data Request. 

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
        experiments   = DR.get_experiments()
        experiments   = sorted(experiments, key=lambda experiment: experiment.name.value)
        opportunities = DR.get_opportunities()
    except Exception as e:
        print(f"Error in accessing Data Request information: {e}")
    return DR, variables, experiments, opportunities


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
    frequency_str = str(getattr(variable, 'cmip7_frequency', None))
    match         = re.search(r':\s*(.*?)\s*\(', frequency_str)

    if match:
        frequency = match.group(1)
        rate = frequency_map.get(frequency)
    else:
        rate = ''
    return rate


def get_vertical_mesh(variable,dreq_tables):
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
     
    vertical_mesh, spatial_shape  = get_vertical_mesh(variable, dreq_tables)
    sampling_rate   = get_sampling_rate(variable)
    horizontal_mesh = 360*180 # 68400

    if sampling_rate != '' and vertical_mesh != '':
        size = sampling_rate * horizontal_mesh * vertical_mesh * 4
    else: 
        size = 0  # Cannot compute; sampling rate or vertical mesh missing
        if name != 'None':
            print(f"Warning: {name} missing sampling_rate or vertical_mesh; size set to 0")

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


def get_varb_list(experiment_varbs):
    varb_names = []
    for varb in experiment_varbs:
        varb_name = varb.cmip6_compound_name.value
        varb_names.append(varb_name)
    return varb_names


def get_ensemble_sizes(DR, opportunity):
    opp_name = opportunity.name.value
    opp = DR.get_opportunity(opp_name)
    opp_expes = DR.find_experiments_per_opportunity(opp_name)

    if experiment in opp_expes:
        min_ensemble_size = opp.get('minimum_ensemble_size')
        desired_ens_size = opp.get('desirable_ensemble_size')
        if isinstance(min_ensemble_size, int):
            min_ensemble_sizes.append(min_ensemble_size)
        if isinstance(desired_ens_size, int):
            desired_ensemble_sizes.append(desired_ens_size)
    unq_min_ensemble_sizes = list(set(min_ensemble_sizes))
    unq_des_ensemble_sizes = list(set(desired_ensemble_sizes))
    unique_ensemble_sizes = list(set(unq_min_ensemble_sizes + unq_des_ensemble_sizes))
    return unique_ensemble_sizes


def define_member_size(unique_ensemble_sizes):
    other_members = []
    if 1 in unique_ensemble_sizes:
        one_member = 1
    else:
        one_member = 0
    if 10 in unique_ensemble_sizes:
        ten_member = 10
    else:
        ten_member = 0
    for size in unique_ensemble_sizes:
        if size != 1 and size != 10:
            other_members.append(size)
    other_members = sorted(other_members)
    return one_member, ten_member, other_members


def calc_tot_vol_per_exp(DR, experiment, opportunities):
    experiment_varbs = get_experiment_varbs(DR, experiment)
    for opportunity in opportunities:
        unique_ensemble_sizes = get_ensemble_sizes(DR, opportunity)
    one_member, ten_member, other_members = define_member_size(unique_ensemble_sizes)
    data_list = []
    if len(experiment_varbs) != 0: 
        run_time         = experiment.get('size_(years,_minimum)')
        # Ensure run_time is numeric
        if not isinstance(run_time, (int, float)):
            print(f"Warning: run_time for experiment {experiment.name.value} is not numeric: {run_time}")
            run_time = 0
        varb_names       = get_varb_list(experiment_varbs)
        all_varb_sizes_per_exp = []
        # count = 0
        for variable in varb_names:
            for item in varbInfoList:
                if item['name'] == str(variable):
                    variable_size_per_decade = item['variable size per decade']
                    # Ensure variable_size_per_decade is numeric before arithmetic
                    if isinstance(variable_size_per_decade, (int, float)) and run_time > 0:
                        varb_size_for_exp = (variable_size_per_decade * run_time) / 10
                        all_varb_sizes_per_exp.append(varb_size_for_exp)
                        # count += varb_size_for_exp
                        # print(item['horizontal mesh']*item['vertical mesh']*item['sampling rate']*4*run_time/10)
                    else:
                        varb_size_for_exp = 0
                        if not isinstance(variable_size_per_decade, (int, float)):
                            print(f"Warning: {item['name']} has non-numeric size: {variable_size_per_decade}")
                    
                    # data = {
                    #     'experiment': experiment.name.value,
                    #     'variable': item['name'],
                    #     'spatial shape': item['spatial shape'],
                    #     'vertical mesh': item['vertical mesh'],
                    #     'horizontal mesh': item['horizontal mesh'],
                    #     'sampling rate': item['sampling rate'],
                    #     'variable size per decade': item['variable size per decade'],
                    #     'runtime': run_time,
                    #     'variable size for experiment (B)': varb_size_for_exp,
                    #     'summing experiment size (B)': count,
                    # }
                    # data_list.append(data)
        vol_for_exp_Bytes = sum(all_varb_sizes_per_exp) # bytes
        vol_for_exp_GB    = vol_for_exp_Bytes / (1024 * 1024 * 1024) 
        vol_for_exp_TB    = vol_for_exp_Bytes / (1024 * 1024 * 1024 * 1024)
        one_member_TB     = one_member * vol_for_exp_TB
        ten_member_TB     = ten_member * vol_for_exp_TB
        expInfo = { 'experiment': experiment.name.value, '# variables': len(varb_names),'volume size (B)': vol_for_exp_Bytes,
                    'volume size (GB)': vol_for_exp_GB ,
                    '1 member (TB)': one_member_TB  ,  '10 members (TB)': ten_member_TB  ,    'Other members': ', '.join(map(str, other_members))  }

    else:
        print("Could not successfully retrieve experiment variables.")
        vol_for_exp_Bytes = 0
        vol_for_exp_GB    = 0
        vol_for_exp_TB    = 0
        expInfo = {}
    return vol_for_exp_Bytes, vol_for_exp_GB, vol_for_exp_TB, expInfo 


def write_all_to_file(data):
    file_path = "testing-variable-info-output.json"
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)
    print(str(len(varbInfoList)) + " variables processed. Variable information can be found in 'testing-variable-info-output.json'")


def create_and_write_table(data, filepath):
    table = pd.DataFrame(data)
    pd.set_option('display.max_rows', None)
    table = pd.concat([table, pd.DataFrame(pd.Series(table.sum(numeric_only= True)).to_dict(),index=['Totals'])])
    table['volume size (B)'] = table['volume size (B)'].apply(lambda x: "{:.0f}".format(x) if pd.notna(x) else '')

    table['# variables'] = table['# variables'].round()
    column_to_skip       = '# variables'
    numeric_cols         = table.select_dtypes(include=np.number).columns
    cols_to_round        = [col for col in numeric_cols if col != column_to_skip]
    table[cols_to_round] = table[cols_to_round].round(3)
    table.to_string(filepath, col_space=18)
    table.to_csv('output.csv', index=False)
    print("\nVolume estimate table written to " + filepath)
    return table


def clean_up_directory():
    cache_dir = Path.home() / ".CMIP7_data_request_api_cache"
    if cache_dir.exists():
        shutil.rmtree(cache_dir)



#### MAIN ####

DR, variables, experiments, opportunities = get_dr_info()
dreq_tables, path_to_content              = load_tables()
experiment_list  = [str(exp.name) for exp in experiments]

varbInfo     = {}
expInfo      = {}
varbInfoList = []
expInfoList  = []
all_volumes  = []

for variable in variables:
    varbInfo = get_varb_data(variable)
    varbInfoList.append(varbInfo)

write_all_to_file(varbInfoList)

for experiment in experiments:
    min_ensemble_sizes     = []
    desired_ensemble_sizes = []
    vol_for_exp_B, vol_for_exp_GB, vol_for_exp_TB, expInfo = calc_tot_vol_per_exp(DR, experiment, opportunities)
    expInfoList.append(expInfo)
    all_volumes.append(vol_for_exp_B)

filepath = 'table_output.txt'
create_and_write_table(expInfoList, filepath)

total_vol_est_B  = sum(all_volumes)
total_vol_est_GB = total_vol_est_B / (1024 * 1024 * 1024 ) 
total_vol_est_TB = total_vol_est_B / (1024 * 1024 * 1024 * 1024)
print(f"\nTotal volume estimate for all experiments: {round(total_vol_est_GB, 4)} GB, {round(total_vol_est_TB, 4)} TB")

# clean_up_directory()


# testing for getting experiment variable volumes (for one experiment)
# experiment = experiments[0]
# vol_for_exp_Bytes, vol_for_exp_GB, vol_for_exp_TB, expInfo, data_list = calc_tot_vol_per_exp(DR, experiment, opportunities)
# all_volumes.append(vol_for_exp_Bytes)

# with open('testing-varb-info-per-exp.json', "w") as file_object:
#     json.dump(data_list, file_object, indent=4)