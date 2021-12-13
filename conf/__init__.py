import os

import yaml

__dev = True

parent_path = os.path.dirname(os.path.realpath(__file__))  # 父文件夹的绝对路径
with open(os.path.join(parent_path, 'config.yaml'), 'r') as f:
    config = yaml.safe_load(f)
    if __dev:
        config = config['dev']
    else:
        config = config['prod']
    task_name = config['task_name']
    target_table_name = config['target_table_name']
    other_table_name = config['other_table_name']
    log_file_path = config['logger']['file_path']
