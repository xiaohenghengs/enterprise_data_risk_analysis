import os

import yaml

item_name = 'enterprise_data_risk_analysis'
cur_path = os.path.abspath(os.path.dirname(__file__))
root_path = os.path.join(cur_path.split(item_name)[0], item_name)
with open(os.path.join(cur_path, 'config.yaml'), 'r') as f:
    config = yaml.safe_load(f)
    task_name = config['task_name']
    target_table_name = config['target_table_name']
    other_table_name = config['other_table_name']
    # database info config
    database = config['database']
    table = database['table']
    oceanbase = database['oceanbase']
