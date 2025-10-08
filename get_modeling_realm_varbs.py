from data_request_api.content import dump_transformation as dt
from data_request_api.query import data_request as dr

## This script creates a text file listing all the variable 
## names associated with the 'Ocean' and 'Ocean Biogeochemistry' modeling realms.
## It pulls data from the data request API version v1.2.2.2.


content_dic = dt.get_transformed_content(version='v1.2.2.2')
DR          = dr.DataRequest.from_separated_inputs(**content_dic)

get_all_variables = DR.get_variables()
all_variables     = list(get_all_variables)

ocean_match     = '[modelling_realm: Ocean (id: ocean)'
ocnBgchem_match = '[modelling_realm: Ocean Biogeochemistry (id: ocnBgchem)'

final_varb_list = []
for var in all_variables:
    if str(var.modelling_realm).startswith(ocean_match) or str(var.modelling_realm).startswith(ocnBgchem_match):
        final_varb_list.append(str(var.cmip6_compound_name))
       # print(str(var.cmip6_compound_name) + ": " + "\n    " + str(var.modelling_realm))

final_varb_list = sorted(final_varb_list)

file_name = "ocean_&_ocnBgchem_variables.txt"
with open(file_name, "w") as file_object:
    file_object.write("\n".join(final_varb_list))