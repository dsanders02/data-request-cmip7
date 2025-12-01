from data_request_api.content import dump_transformation as dt
from data_request_api.query import data_request as dr
import pandas as pd
import json
import warnings
import re
import os


## This script pulls data from the data request API version v1.2.2.2 and creates csv files listing all the variable names associated with the following
## modelling realms: 'Ocean' (ocn) and 'Ocean Biogeochemistry' (ocnBgchem), 'Atmospheric' (atmos), 'Sea Ice' (seaIce), 'Land Surface' (land), and 'Land Ice' (landIce).
## The output files are named accordingly: 'ocean_and_ocnBgchem.csv', 'atmos.csv', 'sea_ice.csv', 'land.csv', and 'land_ice.csv'.
## Each variable entry includes CMIP6 Compund Name, CF Standard Name, Physical Parameter, Title, and Description

warnings.simplefilter("ignore", UserWarning)

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

def match_field(string):
    text = string
    pattern = r":\s*(.*?)\s*\("
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
    description     = re.sub(r"\\_", "_", description)
    physical_param  = str(variable.get('physical_parameter'))
    physical_param  = match_field(physical_param)
    standard_name   = str(variable.get('physical_parameter').get('cf_standard_name'))
    standard_name   = match_field(standard_name)
    if modelling_realm.startswith(atm_match):
        atmVarb = {'CMIP6 Compund Name': variable_name, 'CF Standard Name': standard_name, 'Physical Parameter': physical_param, 'Title': title, 'Description': description }
        atmVarbInfo.append(atmVarb)
    if modelling_realm.startswith(ocean_match) or modelling_realm.startswith(ocnBgchem_match):
        ocnVarb =  {'CMIP6 Compund Name': variable_name, 'CF Standard Name': standard_name, 'Physical Parameter': physical_param, 'Title': title, 'Description': description }
        ocnVarbInfo.append(ocnVarb)
    if modelling_realm.startswith(seaIce_match):
        seaIceVarb =  {'CMIP6 Compund Name': variable_name, 'CF Standard Name': standard_name, 'Physical Parameter': physical_param, 'Title': title, 'Description': description }
        seaIceVarbInfo.append(seaIceVarb)
    if modelling_realm.startswith(land_match):
        landVarb =  {'CMIP6 Compund Name': variable_name, 'CF Standard Name': standard_name, 'Physical Parameter': physical_param, 'Title': title, 'Description': description }
        landVarbInfo.append(landVarb)
    if modelling_realm.startswith(landIce_match):
        landIceVarb = {'CMIP6 Compund Name': variable_name, 'CF Standard Name': standard_name, 'Physical Parameter': physical_param, 'Title': title, 'Description': description }
        landIceVarbInfo.append(landIceVarb)
    return atmVarbInfo, ocnVarbInfo, seaIceVarbInfo, landVarbInfo, landIceVarbInfo


def create_and_write_table(file_name, data):
    path = "modelling_realm_variables/"
    file_path = os.path.join(path, file_name)
    table = pd.DataFrame(data)
    table.to_csv(file_path, index=False)
    
    print(f"{len(data)} variables were wriiten to  {file_path}")
    return table


###  MAIN  ###
DR, all_variables = get_dr_info()

for variable in all_variables:
    atmVarbInfo, ocnVarbInfo, seaIceVarbInfo, landVarbInfo, landIceVarbInfo = get_varb_info(variable)

varbInfo  = {'atmos': atmVarbInfo, 'ocean_and_ocnBgchem': ocnVarbInfo, 'sea_ice': seaIceVarbInfo, 'land': landVarbInfo, 'land_ice': landIceVarbInfo}

for file, varbRealm in varbInfo.items():
    file_name = file + '.csv'
    varbRealm = sorted(varbRealm, key=lambda x: x['CMIP6 Compund Name'])
    create_and_write_table(file_name, varbRealm)