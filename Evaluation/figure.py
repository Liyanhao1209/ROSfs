import json
import matplotlib.pyplot as plt
import numpy as np
import os

import matplotlib.pyplot as plt

plt.rcParams['font.family'] = 'Times New Roman'
plt.rcParams['font.size'] = 18
plt.rcParams['axes.labelsize'] = 18
plt.rcParams['axes.titlesize'] = 18
plt.rcParams['xtick.labelsize'] = 18
plt.rcParams['ytick.labelsize'] = 18
plt.rcParams['legend.fontsize'] = 18

from AOI import aoi_avg

colors = ['#2660A3', '#D0D0D2', '#368ECA', '#9C9EA1', '#757679', '#87C2E1']

def bandwidth(json_file_paths):
    plt.figure(figsize=(12, 8))
    
    markers = ['o', 's', 'D', '^', 'v', '<', '>', 'p', '*', 'h']
    
    for idx, file_path in enumerate(json_file_paths):
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        time_sequence = data['time_sequence']
        
        timestamps = [item[0] for item in time_sequence]
        byte_lengths = [item[2] for item in time_sequence]
        
        cumulative_bytes = 0
        cumulative_data = []
        time_points = []
        
        for i in range(len(timestamps)):
            cumulative_bytes += byte_lengths[i]
            cumulative_data.append(cumulative_bytes)
            time_points.append(timestamps[i])
        
        bandwidth_mbps = []
        for i in range(1, len(cumulative_data)):
            time_diff = time_points[i] - time_points[i-1]
            if time_diff > 0:
                bytes_diff = cumulative_data[i] - cumulative_data[i-1]
                bandwidth_mbps.append((bytes_diff * 8) / (time_diff * 1e6))
            else:
                bandwidth_mbps.append(0)
        
        file_name = os.path.basename(file_path)
        color = colors[idx % len(colors)]
        marker = markers[idx % len(markers)]
        
        plt.plot(time_points[1:], bandwidth_mbps, color=color, marker=marker, 
                markersize=4, linewidth=1.5, label=file_name)
    
    plt.xlabel('Time (seconds)')
    plt.ylabel('Bandwidth (Mbps)')
    # plt.title('Bandwidth Over Time')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig('./bandwidth.pdf')

def aoi(file_paths, split_numbers):
    aoi_values = []
    for file_path in file_paths:
        aoi_val = aoi_avg(file_path)
        aoi_values.append(aoi_val)
    
    plt.figure(figsize=(10, 6))
    
    x_pos = np.arange(len(split_numbers))
    bar_width = 0.6
    
    bars = plt.bar(x_pos, aoi_values, width=bar_width, color=colors, edgecolor='black', linewidth=2)
    
    plt.xlabel('Number of Domains')
    plt.ylabel('AOI Average (seconds)')
    # plt.title('Reconstruction AOI average for different numbers of sub-domains')
    
    plt.xticks(x_pos, split_numbers)
    plt.grid(axis='y', alpha=0.3)
    legend_labels = [f'{num} domains' for num in split_numbers]
    legend = plt.legend(bars, legend_labels, loc='upper center', 
                        bbox_to_anchor=(0.5, 1.20), ncol=4, 
                        frameon=True, handletextpad=0.5, columnspacing=1.0)
    
    # plt.figtext(0.5, 1.02, 'Domains', ha='center', va='bottom', 
    #             fontweight='bold', fontsize=plt.rcParams['legend.fontsize'])
    
    plt.tight_layout()
    
    plt.savefig('aoi_avg.pdf', format='pdf')
    plt.close()
    
def plot_bandwidth():
    json_files = [
        "/data/GroundAir/Evaluation/split/monitor.aoi"
    ]
    
    bandwidth(json_files) 

def plot_aoi():
    json_files = [
        "/data/GroundAir/Evaluation/split/monitor.aoi",
        "/data/GroundAir/Evaluation/split/monitor.aoi",
        "/data/GroundAir/Evaluation/split/monitor.aoi",
        "/data/GroundAir/Evaluation/split/monitor.aoi"
    ]
    
    partitions = [1,4,9,16]
    
    aoi(json_files,partitions)

if __name__ == "__main__":
    # plot_bandwidth()
    plot_aoi()