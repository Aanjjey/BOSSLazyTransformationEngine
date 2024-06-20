import pandas as pd
import json
import sys
from datetime import datetime
import shutil

if len(sys.argv) != 2:
    print("Usage: python script.py <file_name_to_append>")
    sys.exit(1)

with open('build/raw_results/result.json') as f:
    data = json.load(f)

first_benchmark_name = data['benchmarks'][0]['name'].replace('/', '_')
command_line_argument = sys.argv[1]
current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")

file_name = f'{first_benchmark_name}_{command_line_argument}_{current_datetime}'

df = pd.json_normalize(data['benchmarks'])

first_row_run_name = df.loc[0, 'run_name']
run_name = first_row_run_name.split('/', 1)[0]

selected_columns = ['run_name', 'real_time', 'cpu_time']
df_subset = df[selected_columns].copy()

df_subset['run_name'] = df_subset['run_name'].str.split('/').str[1]
df_subset.rename(columns={'run_name': 'input'}, inplace=True)

df_subset.insert(0, 'run_name', run_name)

df_subset.rename(columns={'real_time': 'real_time_' + sys.argv[1], 'cpu_time': 'cpu_time_' + sys.argv[1]}, inplace=True)

df_subset.to_csv(f'results/csv/{file_name}.csv', index=False)

json_file_new_path = f'results/json/{file_name}.json'
shutil.copy('build/raw_results/result.json', json_file_new_path)