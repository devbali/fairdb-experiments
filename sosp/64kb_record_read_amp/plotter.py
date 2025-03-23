import csv
import matplotlib.pyplot as plt
import random

stat_dump_interval_ms = 100
stat_dump_interval_s = stat_dump_interval_ms / 1000
WARMUP = 10
COOLDOWN = 2

def warmup_cooldown (idx):
    lst = list(idx)
    start = 0
    end = 0
    for i in range(len(lst)):
        seconds = lst[i]
        if seconds > WARMUP:
            start = i
            break
    for i in range(len(lst)-1, -1, -1):
        seconds = lst[i]
        if lst[-1] - seconds > COOLDOWN:
            end = i
            break
    return start, end

def plot_client_results(output_file, axs, xlim):
    # Define the file path
    input_file_path = 'logs/client_stats.log'

    client_data = {}
    start_time = None

    # Open and read the CSV file
    with open(input_file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row

        for row in reader:
            timestamp_str, client_id, op_type, count, max_val, min_val, avg, p50, p90, p99, p999, _, _, _, _, _, _, _, _ = row
            timestamp_s = int(timestamp_str) / 1000
            if start_time is None:
                start_time = timestamp_s

            if client_id not in client_data:
                client_data[client_id] = {'Tput': [], '50': [], '99': [], '999': [], 'ts': []}

            multiplier = 1
            if op_type == 'INSERT_BATCH':
                multiplier = 86
            elif op_type == 'SCAN':
                multiplier = 100

            client_data[client_id]['Tput'].append(multiplier * int(count) * 1024 / (1024 * 1024) / stat_dump_interval_s)
            client_data[client_id]['50'].append(float(p50) / 1000)
            client_data[client_id]['99'].append(float(p99) / 1000)
            client_data[client_id]['999'].append(float(p999) / 1000)
            client_data[client_id]['ts'].append(timestamp_s)

    def plot_metric(metric, title, ylabel, fig_loc, ylim):
        #plt.figure(figsize=(8, 5))
        for client_id, stats in client_data.items():
            time_points = [ts - start_time for ts in client_data[client_id]['ts']]
            data_points = [x for x in stats[metric]]
            axs[fig_loc[0]].plot(time_points, data_points, marker='o', label=f'Client {client_id}')
            with open(output_file, 'a') as outfile:
                outfile.write(f"client-{client_id}:metric-{metric}\n")
                outfile.write(f"time_points:{time_points}\n")
                outfile.write(f"data_points:{data_points}\n")

        if metric == 'Tput':
            # Get all the time points:
            all_time_points = [ts for client_id in client_data for ts in client_data[client_id]['ts']]
            all_time_points = sorted(list(set(all_time_points)))

            # For each time point, sum the client throughputs.
            client_tput_sums = []
            for ts in all_time_points:
                cur_sum = 0
                for client_id in client_data:
                    if ts in client_data[client_id]['ts']:
                        idx = client_data[client_id]['ts'].index(ts)
                        cur_sum += client_data[client_id]['Tput'][idx]
                client_tput_sums.append(cur_sum)
            all_time_points = [ts - start_time for ts in all_time_points]
            # axs[fig_loc[0]].plot(all_time_points, client_tput_sums, marker='o', label='Sum')

        axs[fig_loc[0]].set_title(title)
        axs[fig_loc[0]].set_xlabel('Time (s)')
        axs[fig_loc[0]].set_xlim(xlim)
        # axs[fig_loc[0]].set_ylim(0, 40)
        axs[fig_loc[0]].set_ylabel(ylabel)
        axs[fig_loc[0]].legend()
        axs[fig_loc[0]].grid(True)
        # axs[fig_loc[0]].set_ylim(-2, ylim)

    # Plotting each metric
    plot_metric('Tput', 'Client Throughput', 'MB/s', (0, 0), 550)
    plot_metric('50', 'Latency: 50p', '(ms)', (1, 0), 25)
    plot_metric('99', 'Latency: 99p', '99th Percentile (ms)', (2, 0), 2000)
    return start_time


# -----
# Overall throughputs (client + system)

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from datetime import datetime
import time

max_read_tput = 700
max_read_iops = 180000
max_write_tput = 400
max_write_iops = 100000

def timestamp_to_seconds(timestamp_str):
  timestamp = datetime.strptime(timestamp_str.rstrip(), '%Y-%m-%d %H:%M:%S.%f')
  epoch = datetime(1970, 1, 1)
  return (timestamp - epoch).total_seconds()

def plot_overall_tputs(output_file, axs, start_time_shift, xlim, fig_loc):
  df_from_csv = pd.read_csv("iostat_results.csv")

  # print(f"Timestamp: {timestamp_to_seconds(df_from_csv['Timestamp'][0])}")

  time_seconds = np.arange(len(df_from_csv))
  time_seconds = [x + start_time_shift for x in time_seconds]

  # Plotting
  # axs[fig_loc[0]].figure(figsize=(10, 6))
  axs[fig_loc[0]].plot(time_seconds, df_from_csv["rMB/s"], label='Read MB/s', marker='o', color='tab:green')
  axs[fig_loc[0]].plot(time_seconds, df_from_csv["wMB/s"], label='Write MB/s', marker='o', color='tab:red')

  with open(output_file, 'a') as outfile:
    outfile.write(f"metric-rMB_rate\n")
    outfile.write(f"time_points:{time_seconds}\n")
    outfile.write(f"data_points:{df_from_csv['rMB/s'].tolist()}\n")

    outfile.write(f"metric-wMB_rate\n")
    outfile.write(f"time_points:{time_seconds}\n")
    outfile.write(f"data_points:{df_from_csv['wMB/s'].tolist()}\n")
  
  start_idx, end_idx = warmup_cooldown(time_seconds)
  print(f"IO read rate average: {np.array(df_from_csv['rMB/s'].tolist()[start_idx:end_idx]).mean()}")

  axs[fig_loc[0]].set_title('SSD Throughput Over Time')
  axs[fig_loc[0]].set_xlabel('Time (s)')
  axs[fig_loc[0]].set_ylabel('MB/s')
  axs[fig_loc[0]].set_xlim(xlim)
  # axs[fig_loc[0]].set_ylim(0, 520)
  axs[fig_loc[0]].legend(loc='upper left')
  axs[fig_loc[0]].grid(True)

  # # Creating a second y-axis
  # ax2 = axs[fig_loc[0]].twinx()
  # # Plotting on the secondary y-axis
  # ax2.plot(time_seconds, df_from_csv["rMB/s"]/max_read_tput, label='Read Util', marker='x', linestyle='--', color='tab:green')
  # ax2.plot(time_seconds, df_from_csv["wMB/s"]/max_write_tput, label='Write Util', marker='+', linestyle='--', color='tab:red')
  # ax2.set_ylabel('Utilization (based on tput)')
  # ax2.legend(loc='upper right')
  # ax2.set_ylim(0,1)

  # Adjust the right margin to accommodate the second y-axis legend
  plt.subplots_adjust(right=0.85)

  with open(output_file, 'a') as outfile:
    outfile.write(f"metric-rMB_util\n")
    outfile.write(f"time_points:{time_seconds}\n")
    outfile.write(f"data_points:{(df_from_csv['rMB/s']/max_read_tput).tolist()}\n")

    outfile.write(f"metric-wMB_util\n")
    outfile.write(f"time_points:{time_seconds}\n")
    outfile.write(f"data_points:{(df_from_csv['wMB/s']/max_write_tput).tolist()}\n")

def plot_overall_iops(output_file, axs, start_time_shift, xlim, fig_loc):
  df_from_csv = pd.read_csv("iostat_results.csv")
  time_seconds = np.arange(len(df_from_csv))
  time_seconds = [x + start_time_shift for x in time_seconds]

  axs[fig_loc[0]].plot(time_seconds, df_from_csv["r/s"], label='Read IOPS', marker='o', color='tab:green')
  axs[fig_loc[0]].plot(time_seconds, df_from_csv["w/s"], label='Write IOPS ', marker='o', color='tab:red')

  with open(output_file, 'a') as outfile:
    outfile.write(f"metric-rIOP_rate\n")
    outfile.write(f"time_points:{time_seconds}\n")
    outfile.write(f"data_points:{df_from_csv['r/s'].tolist()}\n")

    outfile.write(f"metric-wIOP_rate\n")
    outfile.write(f"time_points:{time_seconds}\n")
    outfile.write(f"data_points:{df_from_csv['w/s'].tolist()}\n")

  axs[fig_loc[0]].set_title('SSD IOPS Over Time')
  axs[fig_loc[0]].set_xlabel('Time (s)')
  axs[fig_loc[0]].set_ylabel('IOPS')
  axs[fig_loc[0]].set_xlim(xlim)
  # axs[fig_loc[0]].set_ylim(0,16)
  axs[fig_loc[0]].legend(loc='upper right')
  axs[fig_loc[0]].grid(True)

  # Creating a second y-axis
  ax2 = axs[fig_loc[0]].twinx()
  # Plotting on the secondary y-axis
  ax2.plot(time_seconds, df_from_csv["r/s"]/max_read_iops, label='Read IOPS Util', marker='x', linestyle='--', color='tab:green')
  ax2.plot(time_seconds, df_from_csv["w/s"]/max_write_iops, label='Write IOPS Util', marker='+', linestyle='--', color='tab:red')
  ax2.set_ylabel('Utilization (based on iops)')
  ax2.legend(loc='upper right')
  ax2.set_ylim(0,1)

  # Adjust the right margin to accommodate the second y-axis legend
  plt.subplots_adjust(right=0.85)

  with open(output_file, 'a') as outfile:
    outfile.write(f"metric-rIOP_util\n")
    outfile.write(f"time_points:{time_seconds}\n")
    outfile.write(f"data_points:{(df_from_csv['r/s']/max_read_iops).tolist()}\n")

    outfile.write(f"metric-wIOP_util\n")
    outfile.write(f"time_points:{time_seconds}\n")
    outfile.write(f"data_points:{(df_from_csv['w/s']/max_write_iops).tolist()}\n")

def plot_io_waittimes(output_file, axs, start_time_shift, xlim, fig_loc):
  df_from_csv = pd.read_csv("iostat_results.csv")
  time_seconds = np.arange(len(df_from_csv))
  time_seconds = [x + start_time_shift for x in time_seconds]

  axs[fig_loc[0]].plot(time_seconds, df_from_csv["r_await"], label='Read Await (per req)', marker='o', color='tab:green')
  # axs[fig_loc[0]].plot(time_seconds, df_from_csv["w_await"], label='Write Await (per req)', marker='o', color='tab:red')

  axs[fig_loc[0]].set_title('IO Wait Times (queueing + servicing)')
  axs[fig_loc[0]].set_xlabel('Time (s)')
  axs[fig_loc[0]].set_ylabel('Wait Time (ms)')
  axs[fig_loc[0]].set_xlim(xlim)
  # axs[fig_loc[0]].set_ylim(0,1)
  axs[fig_loc[0]].legend(loc='upper left')
  axs[fig_loc[0]].grid(True)

  ax2 = axs[fig_loc[0]].twinx()
  ax2.plot(time_seconds, [df_from_csv["r_await"][i] / df_from_csv["rareq-sz"][i] if df_from_csv["rareq-sz"][i] > 0 else df_from_csv["r_await"][i] for i in range(len(df_from_csv["r_await"]))], label='Read Await (per KB)', marker='x', color='tab:green')
  # ax2.plot(time_seconds, [df_from_csv["w_await"][i] / df_from_csv["wareq-sz"][i] if df_from_csv["wareq-sz"][i] > 0 else df_from_csv["w_await"][i]   for i in range(len(df_from_csv["w_await"]))], label='Write Await (per KB)', marker='x', color='tab:red')

  ax2.set_ylabel('IO Wait Times per KB')
  ax2.legend(loc='upper right')
  plt.subplots_adjust(right=0.85)

  with open(output_file, 'a') as outfile:
    outfile.write(f"metric-r_await\n")
    outfile.write(f"time_points:{time_seconds}\n")
    outfile.write(f"data_points:{df_from_csv['r_await'].tolist()}\n")

    outfile.write(f"metric-r_await_per_kb\n")
    outfile.write(f"time_points:{time_seconds}\n")
    outfile.write(f"data_points:{[df_from_csv['r_await'][i] / df_from_csv['rareq-sz'][i] if df_from_csv['rareq-sz'][i] > 0 else df_from_csv['r_await'][i] for i in range(len(df_from_csv['r_await']))]}\n")

def plot_io_reqsize(output_file, axs, start_time_shift, xlim, fig_loc):
  df_from_csv = pd.read_csv("iostat_results.csv")
  time_seconds = np.arange(len(df_from_csv))
  time_seconds = [x + start_time_shift for x in time_seconds]

  axs[fig_loc[0]].plot(time_seconds, df_from_csv["rareq-sz"], label='Avg Read Size', marker='o', color='tab:green')
  axs[fig_loc[0]].plot(time_seconds, df_from_csv["wareq-sz"], label='Avg Write Size', marker='o', color='tab:red')

  axs[fig_loc[0]].set_title('Avg IO Sizes')
  axs[fig_loc[0]].set_xlabel('Time (s)')
  axs[fig_loc[0]].set_ylabel('Size (KB)')
  axs[fig_loc[0]].set_xlim(xlim)
  # axs[fig_loc[0]].set_ylim(0,1)
  axs[fig_loc[0]].legend(loc='upper right')
  axs[fig_loc[0]].grid(True)

  with open(output_file, 'a') as outfile:
    outfile.write(f"metric-r_size\n")
    outfile.write(f"time_points:{time_seconds}\n")
    outfile.write(f"data_points:{df_from_csv['rareq-sz'].tolist()}\n")

    outfile.write(f"metric-w_size\n")
    outfile.write(f"time_points:{time_seconds}\n")
    outfile.write(f"data_points:{df_from_csv['wareq-sz'].tolist()}\n")

# -----

# --------------------------

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Configuration: Operation sizes (in KB)
READ_SIZE_KB = 64  # Size of a single READ operation
INSERT_SIZE_KB = 1  # Size of a single INSERT operation
INSERT_BATCH_SIZE_KB = 100  # Size of a single INSERT_BATCH operation

# Convert sizes to MB for throughput calculations
READ_SIZE_MB = READ_SIZE_KB / 1024
INSERT_SIZE_MB = INSERT_SIZE_KB / 1024
INSERT_BATCH_SIZE_MB = INSERT_BATCH_SIZE_KB / 1024

# Read the CSV file
df = pd.read_csv('logs/client_stats.log')

# Convert timestamp to relative times (in seconds)
df['relative_time_ms'] = df['timestamp'] - df['timestamp'].min()
df['relative_time_s'] = df['relative_time_ms'] / 1000.0

# Sort data and calculate time differences
df = df.sort_values(by=['client_id', 'op_type', 'timestamp'])
df['time_diff_s'] = df.groupby(['client_id', 'op_type'])['relative_time_s'].diff()

# Calculate throughput (ops/sec) and convert to MB/s
df['throughput'] = df['count'] / df['time_diff_s']
df['throughput_mb_s'] = 0.0  # initialize as float

df.loc[df['op_type'] == 'READ', 'throughput_mb_s'] = df['throughput'] * READ_SIZE_MB
df.loc[df['op_type'] == 'INSERT', 'throughput_mb_s'] = df['throughput'] * INSERT_SIZE_MB
df.loc[df['op_type'] == 'INSERT_BATCH', 'throughput_mb_s'] = df['throughput'] * INSERT_BATCH_SIZE_MB

# Drop NaN throughput rows
df_throughput = df.dropna(subset=['throughput_mb_s'])

# Separate dataframes for different operation categories
df_read = df_throughput[df_throughput['op_type'] == 'READ']
df_insert = df_throughput[df_throughput['op_type'] == 'INSERT']
df_insert_batch = df_throughput[df_throughput['op_type'] == 'INSERT_BATCH']
df_queue_latency = df[df['op_type'] == 'QUEUE']
df_read_latency = df[df['op_type'] == 'READ']
df_insert_latency = df[df['op_type'] == 'INSERT']

# Create a copy of df_read to avoid SettingWithCopyWarning
df_read = df_read.copy()

# Calculate user cache total and hit rate
df_read['user_cache_total'] = df_read['user_cache_hits'] + df_read['user_cache_misses']
df_read['user_cache_hit_rate'] = np.where(
    df_read['user_cache_total'] > 0,
    (df_read['user_cache_hits'] / df_read['user_cache_total']) * 100.0,
    np.nan
)

# Prepare data for user_cache_usage stacked area plot
df_cache_usage = df_read[['relative_time_s', 'client_id', 'user_cache_usage']].dropna()
df_cache_usage_pivot = df_cache_usage.pivot_table(
    index='relative_time_s',
    columns='client_id',
    values='user_cache_usage',
    aggfunc='mean',
    fill_value=0
)

# Create figure and axes
fig, axes = plt.subplots(nrows=9, ncols=1, figsize=(10, 24), sharex=True)
fig.subplots_adjust(hspace=0.4)

# 1. Per-Client READ Throughput
ax1 = axes[0]
for client_id, grp in df_read.groupby('client_id'):
    ax1.plot(grp['relative_time_s'], grp['throughput_mb_s'], label=f'Client {client_id}')
ax1.set_title('Per-Client READ Throughput')
ax1.set_ylabel('Throughput (MB/s)')
# ax1.legend(loc='upper right')

ax1 = axes[1]
throughputs = None
idx = None
for client_id, grp in df_read.groupby('client_id'):
    throughputs_new = np.array(grp['throughput_mb_s'])
    length = len(throughputs_new) if throughputs is None else min(len(throughputs), len(throughputs_new))
    throughputs = throughputs_new if throughputs is None else throughputs[:length] + np.array(grp['throughput_mb_s'])[:length]
    idx = grp['relative_time_s'][:length]
ax1.plot(idx, throughputs)
ax1.set_title('Total READ Throughput')
ax1.set_ylabel('Throughput (MB/s)')

# 2. Per-Client WRITE Throughput (INSERT & INSERT_BATCH)
ax2 = axes[2]
for client_id, grp in df_insert.groupby('client_id'):
    ax2.plot(grp['relative_time_s'], grp['throughput_mb_s'], label=f'Client {client_id} INSERT')
for client_id, grp in df_insert_batch.groupby('client_id'):
    ax2.plot(grp['relative_time_s'], grp['throughput_mb_s'], label=f'Client {client_id} INSERT_BATCH')
ax2.set_title('Per-Client WRITE Throughput (INSERT & INSERT_BATCH)')
ax2.set_ylabel('Throughput (MB/s)')
# ax2.legend(loc='upper right')

# 3. Combined READ & WRITE Throughput (in one plot, line per-client per-op)
ax3 = axes[3]
combined_ops = df_throughput[df_throughput['op_type'].isin(['READ', 'INSERT', 'INSERT_BATCH'])]
for (client_id, op_type), grp in combined_ops.groupby(['client_id', 'op_type']):
    ax3.plot(grp['relative_time_s'], grp['throughput_mb_s'], label=f'Client {client_id}, {op_type}')
ax3.set_title('Combined READ & WRITE Throughput')
ax3.set_ylabel('Throughput (MB/s)')
# ax3.legend(loc='upper right')

# 4. P99 READ Latency
ax4 = axes[4]
for client_id, grp in df_read_latency.groupby('client_id'):
    ax4.plot(grp['relative_time_s'], grp['99p'], label=f'Client {client_id}')
ax4.set_title('Per-Client READ P99 Latency')
ax4.set_ylabel('Latency (ms)')
# ax4.legend(loc='upper right')

# 5. P99 INSERT Latency
ax5 = axes[5]
for client_id, grp in df_insert_latency.groupby('client_id'):
    ax5.plot(grp['relative_time_s'], grp['99p'], label=f'Client {client_id}')
ax5.set_title('Per-Client INSERT P99 Latency')
ax5.set_ylabel('Latency (ms)')
# ax5.legend(loc='upper right')

# 6. P99 QUEUE Latency
ax6 = axes[6]
for client_id, grp in df_queue_latency.groupby('client_id'):
    ax6.plot(grp['relative_time_s'], grp['99p'], label=f'Client {client_id}')
ax6.set_title('Per-Client QUEUE P99 Latency')
ax6.set_ylabel('Latency (ms)')
# ax6.legend(loc='upper right')

# 7. User Cache Hit Rate (per-client line)
ax7 = axes[7]
last_grp_hit_rate = None
for client_id, grp in df_read.groupby('client_id'):
    valid_grp = grp.dropna(subset=['user_cache_hit_rate'])
    if not valid_grp.empty:
        last_grp_hit_rate = valid_grp['user_cache_hit_rate']
        ax7.plot(valid_grp['relative_time_s'], valid_grp['user_cache_hit_rate'], label=f'Client {client_id}')
ax7.set_title('Per-Client User Cache Hit Rate')
ax7.set_ylabel('Hit Rate (%)')
# ax7.legend(loc='upper right')

# 8. User Cache Usage (Stacked)
ax8 = axes[8]
x = df_cache_usage_pivot.index
y = df_cache_usage_pivot.values.T  # Each row in y is a series for one client
ax8.stackplot(x, y, labels=df_cache_usage_pivot.columns)
ax8.set_title('Per-Client User Cache Usage (Stacked)')
ax8.set_xlabel('Time (s)')
ax8.set_ylabel('Cache Usage')
ax8.legend(loc='upper right')

start_idx , end_idx = warmup_cooldown(idx)
print(f"""Throughput Client observed: {throughputs[start_idx:end_idx].mean()} mbps
Hit rate of last client: {last_grp_hit_rate[start_idx:end_idx].mean()}%""")


plt.tight_layout()
#plt.show()
plt.savefig('output_plot.png')


def generate_plots(xlim, output_file):
  _, axs = plt.subplots(13, 1, figsize=(12, 50))
  plt.subplots_adjust(hspace=0.4)
  start_time_s = plot_client_results(output_file, axs, xlim)

  start_time_shift = 0
  plot_overall_tputs(output_file, axs, start_time_shift, xlim, (5,0))
  plot_overall_iops(output_file, axs, start_time_shift, xlim, (6,0))
  plot_io_waittimes(output_file, axs, start_time_shift, xlim, (7,0))
  plot_io_reqsize(output_file, axs, start_time_shift, xlim, (8,0))
  plt.savefig('output_big.png')

generate_plots((0,40),"plot.txt")
