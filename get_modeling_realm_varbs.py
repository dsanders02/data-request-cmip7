from data_request_api.content import dump_transformation as dt
from data_request_api.query import data_request as dr
import json
import warnings


## This script creates a text file listing all the variable names associated
## with the 'Ocean' (ocn) and 'Ocean Biogeochemistry' (ocnBgchem) primary 
## modelingrealms, as well as 'Atmospheric' primary modelling realm (atmos).
## It pulls data from the data request API version v1.2.2.2.

warnings.simplefilter("ignore", UserWarning)

def get_dr_info():
    try: 
        content_dic = dt.get_transformed_content(version='v1.2.2.2')
        DR          = dr.DataRequest.from_separated_inputs(**content_dic)
        all_variables = DR.get_variables()
    except Exception as e:
        print(f"Error in accessing Data Request information: {e}")
    return DR, all_variables


def get_varb_info(variable):
    modelling_realm = str(variable.get('modelling_realm'))
    variable_name   = str(variable.get('cmip6_compound_name'))
    title           = str(variable.get('title'))
    description     = str(variable.get('description'))
    if modelling_realm.startswith(atm_match):
        atmVarb = {'name': variable_name, 'title': title, 'description': description, 'modelling realm': modelling_realm }
        atmVarbInfo.append(atmVarb)
    if modelling_realm.startswith(ocean_match) or modelling_realm.startswith(ocnBgchem_match):
        ocnVarb = {'name': variable_name, 'title': title, 'description': description, 'modelling realm': modelling_realm }
        ocnVarbInfo.append(ocnVarb)
    return atmVarbInfo, ocnVarbInfo


def write_json_to_file(file_name, varb_info):
    with open(file_name, "w") as file_object:
        json.dump(varb_info, file_object, indent=4)
    print(f"{len(varb_info)} variables were wriiten to {file_name}")


## MAIN ##
ocean_match     = '[modelling_realm: Ocean (id: ocean)'
ocnBgchem_match = '[modelling_realm: Ocean Biogeochemistry (id: ocnBgchem)'
atm_match       = '[modelling_realm: Atmospheric (id: atmos)'

atmVarb         = {}
atmVarbInfo     = []
ocnVarb         = {}
ocnVarbInfo     = []

DR, all_variables = get_dr_info()

for variable in all_variables:
    atmVarbInfo, ocnVarbInfo     = get_varb_info(variable)

atmVarbInfo = sorted(atmVarbInfo, key=lambda x: x['name'])
ocnVarbInfo = sorted(ocnVarbInfo, key=lambda x: x['name'])

write_json_to_file('ocean_&_ocnBgchem_variables.txt', ocnVarbInfo)
write_json_to_file('atm_variables.txt', atmVarbInfo)