from data_request_api.content import dump_transformation as dt
from data_request_api.query import data_request as dr
import json
import warnings


## This script creates a text file listing all the variable 
## names associated with the 'Ocean' and 'Ocean Biogeochemistry' modeling realms.
## It pulls data from the data request API version v1.2.2.2.

warnings.simplefilter("ignore", UserWarning)

content_dic = dt.get_transformed_content(version='v1.2.2.2')
DR          = dr.DataRequest.from_separated_inputs(**content_dic)

get_all_variables = DR.get_variables()
all_variables     = get_all_variables



ocean_match     = '[modelling_realm: Ocean (id: ocean)'
ocnBgchem_match = '[modelling_realm: Ocean Biogeochemistry (id: ocnBgchem)'
atm_match       = '[modelling_realm: Atmospheric (id: atmos)'

def get_ocn_realm_varbs(variable):
    if str(variable.modelling_realm).startswith(ocean_match) or str(variable.modelling_realm).startswith(ocnBgchem_match):
        ocean_varb_list.append(str(variable.cmip6_compound_name))
    return ocean_varb_list


def get_atm_varbs(variable):
    modelling_realm = str(variable.get('modelling_realm'))
    variable_name   = str(variable.get('cmip6_compound_name'))
    title           = str(variable.get('title'))
    description     = str(variable.get('description'))
    if modelling_realm.startswith(atm_match):
        atmVarb = {'name': variable_name, 'title': title, 'description': description, 'modelling realm': modelling_realm }
        atmVarbInfo.append(atmVarb)
    return atmVarbInfo

def write_text_to_file(file_name, varb_info):
    with open(file_name, "w") as file_object:
        file_object.write("\n".join(varb_info))
    print(f"{len(varb_info)} variables were wriiten to {file_name}")

def write_json_to_file(file_name, varb_info):
    with open(file_name, "w") as file_object:
        json.dump(varb_info, file_object, indent=4)
    print(f"{len(varb_info)} variables were wriiten to {file_name}")


## MAIN ##
ocean_varb_list = []
atmVarb         = {}
atmVarbInfo     = []
for variable in all_variables:
    ocean_varb_list = get_ocn_realm_varbs(variable)
    ocean_varb_list = sorted(ocean_varb_list)
    atmVarbInfo     = get_atm_varbs(variable)
    
write_text_to_file('ocean_&_ocnBgchem_variables.txt', ocean_varb_list)
write_json_to_file('atm_variables.txt', atmVarbInfo)