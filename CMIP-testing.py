#!/usr/bin/env python3

from data_request_api.content import dreq_content as dc
from data_request_api.content import dump_transformation as dt
from data_request_api.query import data_request as dr
from data_request_api.query import dreq_query as dq
from data_request_api.query.dreq_classes import (
    DreqTable, ExptRequest, PRIORITY_LEVELS, format_attribute_name)
import pprint
import pandas as pd
import json


content_dic = dt.get_transformed_content(consolidate=False)
DR = dr.DataRequest.from_separated_inputs(**content_dic)

#print(content_dic)

#path_to_content_json = dc.retrieve('v1.2.2.2')
#print(path_to_content_json)

# attributes = dir(DR)
# print(attributes)
op = DR.get_opportunities()
var_groups = DR.get_variable_groups()[0]
experiment_groups = DR.get_experiment_groups()
#print(experiment_groups[0].id)
#print(len(experiment_groups))

version = dc.get_versions()
#print(version)

used_dreq_version = 'v1.2.2.2'
dreq_content = dc.load(version=used_dreq_version)
dreq_tables = dq.create_dreq_tables_for_request(content=dreq_content, dreq_version=used_dreq_version)
path_to_content = dc._dreq_content_loaded['json_path']

used_cmor_tables = ['E3hr']
metadata1 = dq.get_variables_metadata(content=dreq_tables, cmor_tables=used_cmor_tables, 
                                      dreq_version=used_dreq_version)

used_api_version = 'v1.2.2.2'
metadata1
json_out1 = 'var_attrs_2tables.json'
dq.write_variables_metadata(metadata1, dreq_version=used_dreq_version, filepath=json_out1, 
                            api_version=used_api_version, content_path=path_to_content)


data = json.loads(json_out1)
print(f"{{data['Header']}}")
#df = pd.read_json(json_out1)
#print(df)


#ops_per_expr = DR.find_opportunities_per_experiment('1pctCO2')
#pprint.pprint(ops_per_expr[0].attributes)
#print(len(ops_per_expr))
#op = DR.get_opportunity(id='dafc73bd-8c95-11ef-944e-41a8eb05f654').attributes
#op_description = getattr(op, 'description', None)
#op_id = getattr(op, 'opportunity_id', None)


#print(getattr(d, 'value', getattr(d, 'text', getattr(d, 'label', None))))
#print(DR.get_opportunities())
#pprint.pprint((DR.get_opportunity(id='dafc73bd-8c95-11ef-944e-41a8eb05f654')).description)

# nb_print=5

# all_opportunities = DR.get_opportunities()
# list_size = len(all_opportunities)
# print(list_size)
# print(*all_opportunities[:min(nb_print, list_size)], sep='\n')

# used_opps = ["Clouds, radiation & precipitation"]
# filters = {"opportunity":used_opps}


# expes = DR.find_experiments(operation='any', skip_if_missing=True, **filters)
# #print(expes)

# all_experiments = DR.get_experiments()
# first_expe = all_experiments[0]

# print(first_expe.opportunity)
