from data_request_api.content import dump_transformation as dt
from data_request_api.query import data_request as dr
import argparse
import os
import warnings


warnings.simplefilter("ignore", UserWarning)
debug = False

def dprint(msg):
    if debug == True:
        print(msg)

def get_dr_info():
    dprint("Getting DR info...")
    content_dic = dt.get_transformed_content(version='v1.2.2.2')
    DR          = dr.DataRequest.from_separated_inputs(**content_dic)
    return DR

def get_list_of_experiments(DR):
    dprint("Getting list of experiments...")
    experiments = DR.get_experiments()
    # print(experiments[0].get('size_(years,_minimum)'))
    experiment_list = [str(exp.name) for exp in experiments]
    return experiment_list

def get_experiment_varbs(DR, experiment_name):
    dprint(f"Getting variables for experiment")
    filter = {"experiments": [experiment_name]}
    try: 
        experiment_varbs = DR.find_variables(operation='any', skip_if_missing=True, **filter)
    except Exception as e:
        print(f"Error occurred while fetching variables for experiment {experiment_name}: {e}")
        experiment_varbs = []
    return experiment_varbs

def get_varb_name_list(experiment_varbs):
    dprint("Getting variable names...")
    varb_names = []
    for varb in experiment_varbs:
        varb_name = str(varb.cmip6_compound_name)
        varb_names.append(varb_name)
    if experiment_name == "all":
        print(str(len(varb_names)) + " variables found for experiment " + exp)
    else:
        print(str(len(varb_names)) + " variables found for experiment " + experiment_name)
    return varb_names

def write_varb_list_to_file(varb_names, experiment_name):
    dprint("Writing variable names to file...")
    path = "experiment_variables/"
    file_name = experiment_name + ".txt"
    file_path = os.path.join(path, file_name)
    os.makedirs(path, exist_ok=True)
    with open(file_path, "w") as file_object:
        file_object.write("\n".join(sorted(varb_names)))
    print(f"Variable names written to {file_path}")


## MAIN ##
parser = argparse.ArgumentParser(description='''This script retrieves variables linked to the given experiment from the data request API.''')
parser.add_argument('--experiment',   '-E', help='Experiment name',       required=True)
parser.add_argument('--verbose','-v', help='turn on debug messages', action='store_true')
args = parser.parse_args()
if args.experiment:
    experiment_name = str(args.experiment)
if args.verbose:
    debug = True

DR = get_dr_info()
experiment_list = get_list_of_experiments(DR)

if experiment_name == "all":
    for exp in experiment_list:
        experiment_varbs = get_experiment_varbs(DR, exp)
        varb_names       = get_varb_name_list(experiment_varbs)
        write_varb_list_to_file(varb_names, exp)
elif experiment_name in experiment_list:
    experiment_varbs = get_experiment_varbs(DR, experiment_name)
    varb_names       = get_varb_name_list(experiment_varbs)
    write_varb_list_to_file(varb_names, experiment_name)
else:
    print(f"No variables found for experiment {experiment_name}. No file created. Check for typos in the experiment name.")

