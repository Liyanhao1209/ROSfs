import json
import matplotlib.pyplot as plt
import numpy as np
import os
from scipy.signal import find_peaks

import matplotlib.pyplot as plt

plt.rcParams['font.family'] = 'Times New Roman'
plt.rcParams['font.size'] = 18
plt.rcParams['axes.labelsize'] = 18
plt.rcParams['axes.titlesize'] = 18
plt.rcParams['xtick.labelsize'] = 18
plt.rcParams['ytick.labelsize'] = 18
plt.rcParams['legend.fontsize'] = 18

import json
import matplotlib.pyplot as plt
import numpy as np
import os

def bandwidth(json_files):
    plt.figure(figsize=(12, 8))
    
    colors = plt.cm.tab10(np.linspace(0, 1, len(json_files)))
    
    for i, file_path in enumerate(json_files):
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        time_sequence = data['time_sequence']
        
        times = [item[0] for item in time_sequence]
        byte_lengths = [item[2] for item in time_sequence]
        
        bandwidths = []
        time_points = []
        
        for j in range(len(times)):
            if j == 0:
                continue
            
            time_diff = times[j] - times[j-1]
            if time_diff > 0:
                bandwidth = (byte_lengths[j] * 8) / (time_diff * 1e6)
                bandwidths.append(bandwidth)
                time_points.append(times[j])
        
        file_name = os.path.basename(file_path)
        plt.plot(time_points, bandwidths, color=colors[i], linewidth=2, label=file_name, alpha=0.8)
    
    plt.xlabel('Time (seconds)', fontsize=12)
    plt.ylabel('Bandwidth (Mbps)', fontsize=12)
    plt.title('Bandwidth Over Time', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=10)
    plt.tight_layout()
    plt.savefig('./bandwidth.png')
    
    return plt

def plot_bandwidth(json_file_paths):
    plt.figure(figsize=(12, 8))
    
    colors = ['blue', 'red', 'green', 'orange', 'purple', 'brown', 'pink', 'gray', 'olive', 'cyan']
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
    plt.title('Bandwidth Over Time')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig('./bandwidth.png')

if __name__ == "__main__":
    json_files = [
        "/data/GroundAir/Evaluation/split/monitor.aoi"
    ]
    
    plot_bandwidth(json_files) 