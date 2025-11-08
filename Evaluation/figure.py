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
                markersize=4, linewidth=1.5, label=file_name, zorder=2)
    
    plt.grid(axis='y', linewidth=1, color='black', alpha=0.5, zorder=3)
    plt.xlabel('Time (seconds)')
    plt.ylabel('Bandwidth (Mbps)')
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
    bar_width = 0.1
    
    bars = plt.bar(x_pos, aoi_values, width=bar_width, color=colors, edgecolor='black', linewidth=2, zorder=2)
    
    plt.grid(axis='y', linewidth=1, color='black', alpha=0.5, zorder=3)
    
    plt.xlabel('Number of Domains')
    plt.ylabel('AOI Average (seconds)')
    
    plt.xlim(-0.5, len(split_numbers)-0.5)
    
    plt.xticks(x_pos, split_numbers)
    legend_labels = [f'{num} domains' for num in split_numbers]
    legend = plt.legend(bars, legend_labels, loc='upper center', 
                        bbox_to_anchor=(0.5, 1.20), ncol=4, 
                        frameon=True, handletextpad=0.5, columnspacing=1.0)
    
    plt.tight_layout()
    
    plt.savefig('aoi_avg.pdf', format='pdf')
    plt.close()

def completion_time(data_list, split_numbers):
    sampling_times = []
    pose_times = []
    reconstruction_times = []
    merging_times = []
    
    for i, data in enumerate(data_list):
        if i == 0:
            sampling_times.append(data[0])
            pose_times.append(data[1])
            reconstruction_times.append(data[2])
            merging_times.append(0)
        else:
            sampling_times.append(data[0])
            reconstruction_times.append(data[1])
            merging_times.append(data[2])
            pose_times.append(0)
    
    x_pos = np.arange(len(split_numbers))
    bar_width = 0.5
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    bottom = np.zeros(len(split_numbers))
    
    sampling_bars = ax.bar(x_pos, sampling_times, bar_width, label='Sampling', 
                           color=colors[0], edgecolor='black', linewidth=2, zorder=2)
    
    bottom += sampling_times
    
    pose_bars = ax.bar(x_pos, pose_times, bar_width, bottom=bottom, 
                       label='Pose Computation', color=colors[1], 
                       edgecolor='black', linewidth=2, zorder=2)
    
    bottom += pose_times
    
    reconstruction_bars = ax.bar(x_pos, reconstruction_times, bar_width, 
                                 bottom=bottom, label='Reconstruction', 
                                 color=colors[2], edgecolor='black', linewidth=2, zorder=2)
    
    bottom += reconstruction_times
    
    merging_bars = ax.bar(x_pos, merging_times, bar_width, bottom=bottom, 
                          label='Merging', color=colors[3], 
                          edgecolor='black', linewidth=2, zorder=2)
    
    ax.grid(axis='y', linewidth=1, color='black', alpha=0.5, zorder=3)
    ax.set_xlabel('Number of Domains')
    ax.set_ylabel('Completion Time (seconds)')
    ax.set_xticks(x_pos)
    ax.set_xticklabels(split_numbers)
    
    max_height = max(bottom)
    ax.set_ylim(0, max_height * 1.1)
    
    ax.legend(loc='upper right', frameon=True)
    
    plt.tight_layout()
    plt.savefig('completion_time.pdf', format='pdf', bbox_inches='tight')
    plt.close()
    

def plot_completion():
    mock_data = [
        [120, 45, 180],  
        [25, 20, 35],     
        [18, 15, 28],     
        [15, 10, 22],    
        [12, 5, 18]      
    ]
     
    partitions = [1,4,9,16,25]
    
    completion_time(mock_data,partitions)

def plot_bandwidth():
    json_files = [
        "/data/GroundAir/Evaluation/split/monitor.aoi"
    ]
    
    bandwidth(json_files) 

def plot_aoi():
    json_files = [
        "/data/GroundAir/Evaluation/split/monitor.aoi",
    ]
    
    partitions = [4]
    
    aoi(json_files,partitions)

if __name__ == "__main__":
    plot_bandwidth()
    plot_aoi()
    plot_completion()