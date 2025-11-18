from data_request_api.content import dump_transformation as dt
from data_request_api.query import data_request as dr
import json
import warnings
import re
import pprint
import os


## This script pulls data from the data request API version v1.2.2.2 and creates a text file listing all the variable names associated
## with the following modelling realms: 'Ocean' (ocn) and 'Ocean Biogeochemistry' (ocnBgchem), 'Atmospheric' (atmos), 'Sea Ice' (seaIce), 'Land Surface' (land), and 'Land Ice' (landIce).
## The output files are named accordingly: 'ocean_and_ocnBgchem_variables.txt', 'atm_variables.txt', 'sea_ice_variables.txt', 'land_surface_variables.txt', and 'land_ice_variables.txt'.
## Each variable entry includes its standard name, title, and description.

warnings.simplefilter("ignore", UserWarning)

ocean_match     = '[modelling_realm: Ocean (id: ocean)'
ocnBgchem_match = '[modelling_realm: Ocean Biogeochemistry (id: ocnBgchem)'
atm_match       = '[modelling_realm: Atmospheric (id: atmos)'
seaIce_match    = '[modelling_realm: Sea Ice (id: seaIce)'
land_match      = '[modelling_realm: Land Surface (id: land)'
landIce_match   = '[modelling_realm: Land Ice (id: landIce)'

def get_dr_info():
    try: 
        content_dic = dt.get_transformed_content(version='v1.2.2.2')
        DR          = dr.DataRequest.from_separated_inputs(**content_dic)
        all_variables = DR.get_variables()
    except Exception as e:
        print(f"Error in accessing Data Request information: {e}")
    return DR, all_variables

def get_name(string):
    text = string
    pattern = r"cf_standard_name:\s*(.*?)\s*\("
    match = re.search(pattern, text)
    if match:   
        extracted_string = match.group(1)
    else:
        extracted_string = None
    return extracted_string

def get_varb_info(variable):
    modelling_realm = str(variable.get('modelling_realm'))
    variable_name   = str(variable.get('cmip6_compound_name'))
    title           = str(variable.get('title'))
    description     = str(variable.get('description'))
    standard_name   = str(variable.get('physical_parameter').get('cf_standard_name'))
    standard_name   = get_name(standard_name)
    if modelling_realm.startswith(atm_match):
        atmVarb = {'CMIP6 Compund Name': variable_name, 'Standard Name': standard_name, 'Title': title, 'Description': description}
        atmVarbInfo.append(atmVarb)
    if modelling_realm.startswith(ocean_match) or modelling_realm.startswith(ocnBgchem_match):
        ocnVarb = {'CMIP6 Compund Name': variable_name, 'Standard Name': standard_name, 'Title': title, 'Description': description}
        ocnVarbInfo.append(ocnVarb)
    if modelling_realm.startswith(seaIce_match):
        seaIceVarb = {'CMIP6 Compund Name': variable_name, 'Standard Name': standard_name, 'Title': title, 'Description': description}
        seaIceVarbInfo.append(seaIceVarb)
    if modelling_realm.startswith(land_match):
        landVarb = {'CMIP6 Compund Name': variable_name, 'Standard Name': standard_name, 'Title': title, 'Description': description}
        landVarbInfo.append(landVarb)
    if modelling_realm.startswith(landIce_match):
        landIceVarb = {'CMIP6 Compund Name': variable_name, 'Standard Name': standard_name, 'Title': title, 'Description': description}
        landIceVarbInfo.append(landIceVarb)
    return atmVarbInfo, ocnVarbInfo, seaIceVarbInfo, landVarbInfo, landIceVarbInfo


def write_json_to_file(file_name, varb_info):
    path = "modelling_realm_variables/"
    file_path = os.path.join(path, file_name)
    os.makedirs(path, exist_ok=True)
    with open(file_path, "w") as file_object:
        json.dump(varb_info, file_object, indent=4)
    print(f"{len(varb_info)} variables were wriiten to  {file_path}")


###  MAIN  ###
atmVarb         = {}
atmVarbInfo     = []
ocnVarb         = {}
ocnVarbInfo     = []
seaIceVarb      = {}
seaIceVarbInfo  = []
landVarb        = {}
landVarbInfo    = []
landIceVarb     = {}
landIceVarbInfo = []

DR, all_variables = get_dr_info()

for variable in all_variables:
    atmVarbInfo, ocnVarbInfo, seaIceVarbInfo, landVarbInfo, landIceVarbInfo = get_varb_info(variable)

atmVarbInfo     = sorted(atmVarbInfo,     key=lambda x: x['CMIP6 Compund Name'])
ocnVarbInfo     = sorted(ocnVarbInfo,     key=lambda x: x['CMIP6 Compund Name'])
seaIceVarbInfo  = sorted(seaIceVarbInfo,  key=lambda x: x['CMIP6 Compund Name'])
landVarbInfo    = sorted(landVarbInfo,    key=lambda x: x['CMIP6 Compund Name'])
landIceVarbInfo = sorted(landIceVarbInfo, key=lambda x: x['CMIP6 Compund Name'])

write_json_to_file('ocean_and_ocnBgchem_variables.txt', ocnVarbInfo)
write_json_to_file('atm_variables.txt', atmVarbInfo)
write_json_to_file('sea_ice_variables.txt', seaIceVarbInfo)
write_json_to_file('land_variables.txt', landVarbInfo)
write_json_to_file('land_ice_variables.txt', landIceVarbInfo)