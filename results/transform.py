import json
import matplotlib.pyplot as plt
import numpy as np
import os

# Create a folder to save the graph
output_folder = 'graphs'
os.makedirs(output_folder, exist_ok=True)

# Load the JSON data from the files
with open('line_transformed.json', 'r') as file:
    line_transformed_data = json.load(file)

with open('butterfly_transformed.json', 'r') as file:
    butterfly_transformed_data = json.load(file)

with open('transformationTime.json', 'r') as file:
    paste_data = json.load(file)

# Extract the relevant data for each size
sizes = ['1MB', '10MB', '100MB', '1000MB', '10000MB', '20000MB']

data_groups = []

# Find the setup times for line and butterfly transformation
line_transform_setup = next((b for b in line_transformed_data['benchmarks'] if b['name'] == 'LINE_TRANSFORMATION_SETUP//process_time/real_time'), None)
butterfly_transform_setup = next((b for b in butterfly_transformed_data['benchmarks'] if b['name'] == 'BUTT_TRANSFORMATION_SETUP//process_time/real_time'), None)

if line_transform_setup and butterfly_transform_setup:
    data_groups.append([
        line_transform_setup['real_time'] / 1e9,
        butterfly_transform_setup['real_time'] / 1e9
    ])

for size in sizes:
    # Find the relevant benchmarks for each data source
    line_transform = next((b for b in paste_data['benchmarks'] if f'Line/{size}' in b['name']), None)
    butterfly_transform = next((b for b in paste_data['benchmarks'] if f'/{size}' in b['name'] and 'Line' not in b['name']), None)

    if line_transform and butterfly_transform:
        data_groups.append([
            line_transform['real_time'] / 1e9,
            butterfly_transform['real_time'] / 1e9
        ])

if data_groups:
    # Create the bar chart
    fig, ax = plt.subplots(figsize=(10, 6))
    x = np.arange(len(data_groups))
    width = 0.35

    # Plot the bars for each data group
    ax.bar(x, [group[0] for group in data_groups], width, label='Line')
    ax.bar(x + width, [group[1] for group in data_groups], width, label='Butterfly')

    # Set the chart title and labels
    ax.set_title('Transformation Time Comparison')
    ax.set_xticks(x + width / 2)
    # ax.set_xticklabels(['Setup'] + sizes[:len(data_groups) - 1])  # Adjust the number of labels
    ax.set_ylabel('Transformation Time (seconds)')
    ax.set_yscale('log')
    ax.legend()
    # Set y-axis tick format to display values in normal format
    ax.ticklabel_format(style='plain', axis='y')

    # Save the chart to the output folder
    output_filename = f'/Users/anjey/Desktop/graphs1/transformation_time_comparison.png'
    plt.savefig(output_filename)

    # Close the figure to free up memory
    plt.close(fig)
