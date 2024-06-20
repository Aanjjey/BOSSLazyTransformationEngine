import json
import matplotlib.pyplot as plt
import numpy as np

# Load the JSON data from the files
with open('baseline_no_transformation.json', 'r') as file:
    baseline_data = json.load(file)

with open('butterfly_transformed.json', 'r') as file:
    butterfly_transformed_data = json.load(file)

with open('line_transformed.json', 'r') as file:
    line_transformed_data = json.load(file)

# Extract the relevant data for each percentage point
percentage_points = [20, 40, 60, 80, 100]
sf_values = [1, 10, 100, 1000, 10000, 20000]

for percentage in percentage_points:
    data_groups = []

    for sf in sf_values:
        # Find the relevant benchmarks for each data source
        line3_transform = next((b for b in line_transformed_data['benchmarks'] if f'LINE_03_TR_SF_{sf}PC{percentage}' in b['name']), None)
        line3_no_transform = next((b for b in baseline_data['benchmarks'] if f'LINE_03_TR_SF_{sf}PC{percentage}' in b['name']), None)
        line4_transform = next((b for b in line_transformed_data['benchmarks'] if f'LINE_04_TR_SF_{sf}PC{percentage}' in b['name']), None)
        line4_no_transform = next((b for b in baseline_data['benchmarks'] if f'LINE_04_TR_SF_{sf}PC{percentage}' in b['name']), None)
        butterfly12_transform = next((b for b in butterfly_transformed_data['benchmarks'] if f'BUTT_12_TR_SF_{sf}PC{percentage}' in b['name']), None)
        butterfly12_no_transform = next((b for b in baseline_data['benchmarks'] if f'BUTT_12_TR_SF_{sf}PC{percentage}' in b['name']), None)
        butterfly13_transform = next((b for b in butterfly_transformed_data['benchmarks'] if f'BUTT_13_TR_SF_{sf}PC{percentage}' in b['name']), None)
        butterfly13_no_transform = next((b for b in baseline_data['benchmarks'] if f'BUTT_13_TR_SF_{sf}PC{percentage}' in b['name']), None)

        data_groups.append([
            # line3_transform['real_time'] / 1e9,
            # line3_no_transform['real_time'] / 1e9,
            # line4_transform['real_time'] / 1e9,
            # line4_no_transform['real_time'] / 1e9
            butterfly12_transform['real_time'] / 1e9,
            butterfly12_no_transform['real_time'] / 1e9,
            butterfly13_transform['real_time'] / 1e9,
            butterfly13_no_transform['real_time'] / 1e9
        ])

    if data_groups:
        # Create the bar chart
        fig, ax = plt.subplots(figsize=(10, 6))
        x = np.arange(len(data_groups))
        width = 0.2

        # Plot the bars for each data group
        # bar_labels = ['Line3 (Transform)', 'Line3 (No Transform)', 'Butterfly (Transform)', 'Butterfly (No Transform)',]
        # bar_labels = ['Line3 (Transform)', 'Line3 (No Transform)', 'Line4 (Transform)', 'Line4 (No Transform)']
        bar_labels = ['Butterfly12 (Transform)', 'Butterfly12 (No Transform)', 'Butterfly13 (Transform)', 'Butterfly13 (No Transform)']
        for i in range(4):
            ax.bar(x + i * width, [group[i] for group in data_groups], width, label=bar_labels[i])

        # Set the chart title and labels
        ax.set_title(f'Real Time Comparison for {percentage}% Percentage Point')
        ax.set_xticks(x + 1.5 * width)
        ax.set_xticklabels([f'SF {sf}' for sf in sf_values[:len(data_groups)]])
        ax.set_ylabel('Real Time')
        ax.ticklabel_format(style='plain', axis='y')
        ax.legend()

        # Set exponential scale for the y-axis
        ax.set_yscale('log')

        # Create a legend outside the plot area
        ax.legend(bar_labels, loc='center left', bbox_to_anchor=(1, 0.5))

        plt.tight_layout()
        # plt.show()

        output_filename = f'/Users/anjey/Desktop/graphs1/butterflies_{percentage}_percent.png'
        plt.savefig(output_filename)
        plt.close(fig)
