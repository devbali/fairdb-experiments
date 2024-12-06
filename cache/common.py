try:
    import pandas as pd
    import matplotlib.pyplot as plt
    import numpy as np
except ModuleNotFoundError:
    pass

remove_outliers = lambda d: d[np.abs((d - d.mean()) / d.std()) < 5]
#remove_outliers = lambda d: d[d < d.quantile(0.98)]
filterer = lambda d: remove_outliers(d[d['client_id'] == 0].iloc[20:-5]['avg'])

def get_args(num_clients):
    return {
        "recordcount": [10000]*num_clients,
        "operationcount": 1000000,
        "fieldcount": 16,
        "fieldlength": 1024//16, # total record size is 16 * 65536, which is 1024 * 1024, which is 1 MB
        "rocksdb.num_cfs": num_clients,
        "target_rates": [10000] * num_clients, # 10k requests a second
        "read_rate_limits": [500] * num_clients, # 500 mb per second
        "rocksdb.cache_size": [1024*1024 * 40] * num_clients,
        "io_read_capacity_kbps": 1024 * 500 * num_clients,

        "rsched": False,
        "refill_period": 5,
        "rsched_interval_ms": 50,
        "lookback_intervals": 30,
        "rsched_rampup_multiplier": "1.2",
        "burst_gap_s": 0,
        "burst_size_ops": 1,

        "io_write_capacity_kbps": 10,
        "memtable_capacity_kb": 10,
        "max_memtable_size_kb": 10,
        "min_memtable_size_kb": 10,
        "min_memtable_count": 1,

        "client_to_cf_map": ["default"] + [f"cf{i}" for i in range(1, num_clients)],
        "client_to_cf_offset": [0] * num_clients,

        "rocksdb.disable_auto_compactions": True,
        "rocksdb.compression_per_level": ['kSnappyCompression']* num_clients,
        "rocksdb.max_write_buffer_number": [20] * num_clients,
        "rocksdb.min_write_buffer_number_to_merge": [1] * num_clients,
        "rocksdb.write_buffer_size": [67108864] * num_clients,

        "tpool_threads": 256,
        "requestdistribution": ["uniform"] * num_clients,
        "status.interval_ms": 100,

        "fairdb_use_pooled": False,
        "fairdb_cache_rad": 100,
        "cache_num_shard_bits": -1,
        "ramp_duration": [0] * num_clients,
        "ramp_start": [0] * num_clients
    }

import os
def run_cmd (cmd):
    print(cmd)
    os.system(f"cd ~/project/YCSB-cpp && {cmd}")

def add_args_to_cmd (cmd, args, destfile="/tmp/output.txt"):
    for arg in args:
        if isinstance(args[arg], list):
            lst = [str(a) for a in args[arg]]
            strarg = f'"{",".join(lst)}"'
            if arg == "target_rates":
                cmd += f'-target_rates {strarg}  '
                continue
        elif isinstance(args[arg], bool):
            strarg = "true" if args[arg] else "false"
        else:
            strarg = str(args[arg])
        cmd += f'-p {arg}={strarg}  '
    cmd += '-s'
    if destfile: cmd += f" >{destfile}"
    return cmd

def do_load (args, num_tables):
    run_cmd("rm -rf /mnt/rocksdb/ycsb-rocksdb-data")
    for i in range(num_tables):
        if i == 0:
            tablename = "default"
        else:
            tablename = f"cf{i}"

        cmd = f"""./ycsb \
        -load \
        -db rocksdb \
        -P workloads/workloada \
        -P rocksdb/rocksdb.properties \
        -p rocksdb.dbname=/mnt/rocksdb/ycsb-rocksdb-data \
        -p table={tablename} \
        """
        args_load = {**args}
        del args_load["target_rates"]
        args_load["fairdb_use_pooled"] = False

        run_cmd(add_args_to_cmd(cmd, args_load))

def do_run (args, output=False):
    num_tables = args["rocksdb.num_cfs"]
    cmd = f"""./ycsb \
        -run \
        -threads {num_tables} \
        -db rocksdb \
        -P workloads/workloada \
        -p readproportion=1 \
        -p insertproportion=0 \
        -p updateproportion=0 \
        -P rocksdb/rocksdb.properties \
        -p rocksdb.dbname=/mnt/rocksdb/ycsb-rocksdb-data \
        """

    run_cmd(add_args_to_cmd(cmd, args, destfile="" if output else "/tmp/output.txt"))
    try:
        return pd.read_csv("~/project/YCSB-cpp/logs/client_stats.log")
    except NameError:
        print("done")

def transform_timestamp_series (s):
    m = s.min()
    return pd.Series(list(s)).apply(lambda s: (s-m)/1000)

ROLLING_WINDOW = 5
def time_series_line_graph (ys, x_label, s_label, dest, colors, y_label):
    fig, ax = plt.subplots()
    for i in range(len(ys)):
        y = ys[i]
        ax.plot(transform_timestamp_series(y.index), y, label = f'Client {i}', color=colors[i])

    ax.set_xlabel('Time (s)')
    ax.set_ylabel(y_label)
    ax.set_title(f"Cache in {x_label} {s_label}")

    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    fig.set_size_inches(12, 6)
    plt.savefig(dest)

def plot_field (df, x_label, s_label, dest, colors, FIELD = "99p"):
    ys = []
    for client_id in sorted(df["client_id"].unique()):
        y = df[df["client_id"] == client_id][["timestamp", FIELD]].set_index("timestamp").rolling(window=ROLLING_WINDOW).mean()
        ys.append(y)

    time_series_line_graph(ys, x_label, s_label, 
        dest.replace(".png", f"{FIELD}_{s_label.replace('/','-')}_{x_label.replace('/','-')}.png"), colors, FIELD)

def plot_cache_allocs(df, x_label, s_label, dest):
    if df.iloc[0]["user_cache_usage"] == 0:
        return
    real_dest = dest.replace(".png", f"_{s_label.replace('/','-')}_{x_label.replace('/','-')}.png")

    ys = []
    df["time_s"] = transform_timestamp_series(df["timestamp"])
    for client_id in sorted(df["client_id"].unique()):
        ys.append(df[df["client_id"] == client_id].set_index("time_s")["user_cache_usage"].apply(lambda c: abs(c)/(2**30)))
    
    time_series = ys[0].index
    for y in ys[1:]:
        time_series = time_series.union(y.index)

    interpolated_series = []
    for series in ys:
        series_interp = series.reindex(time_series).interpolate(method='linear').fillna(0)
        interpolated_series.append(series_interp)

    fig, ax = plt.subplots()
    ax.stackplot(
        time_series, 
        *interpolated_series, 
        labels=[f'Client {i}' for i in range(len(ys))],
        baseline="zero"
    )

    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Cache capacity (GB)')
    ax.set_title(f"Cache in {x_label} {s_label}")

    ax.legend()
    fig.set_size_inches(12, 6)
    plt.savefig(real_dest)

def plot_hit_rate (df, x_label, s_label, dest, colors):
    FIELDS = ["user_cache_hits", "user_cache_misses"]
    if df.iloc[-1][FIELDS[0]] == 0:
        FIELDS = ["global_cache_hits", "global_cache_misses"]
    if df.iloc[-1][FIELDS[0]] == 0:
        return
    real_dest = dest.replace(".png", f"hit_rate_{s_label.replace('/','-')}_{x_label.replace('/','-')}.png")

    ys = []
    for client_id in sorted(df["client_id"].unique()):
        cum_arr_dict = {"timestamp": df[df["client_id"] == client_id]["timestamp"]}
        for f in FIELDS:
            cum_arr_dict[f] = np.diff(df[df["client_id"] == client_id][f], prepend=0)
        cumulative_arrays = pd.DataFrame(cum_arr_dict)
        ys.append(cumulative_arrays.set_index("timestamp").apply(lambda r: 100 * r[FIELDS[0]] / (r[FIELDS[0]] + r[FIELDS[1]]), axis=1).rolling(window=ROLLING_WINDOW).mean())

    time_series_line_graph(ys, x_label, s_label, 
        dest.replace(".png", f"hit_rate_{s_label.replace('/','-')}_{x_label.replace('/','-')}.png"), colors, 'Hit Rate (%)')


def plot_data(labels=[], data=[], f=lambda d: d['avg'].mean(), err_f=lambda d: d['std'].mean(),
    series_labels=['Isolation', 'Pooled'],
    x_label="Config",
    y_label="Average (Mean) Latency",
    title='Affect of Comparative Request Rate on Latency in RocksDB Block Cache',
    dest=f"graph.png",
    colors=[]):
    
    seriess = {s: [] for s in series_labels}
    errors = {s: [] for s in series_labels}
    for ri in range(len(data)):
        run = data[ri]
        #print(run)
        for sindex in range(len(series_labels)):
            s = series_labels[sindex]
            val = f(run[sindex])
            plot_cache_allocs(run[sindex], labels[ri], s, dest)
            plot_hit_rate(run[sindex], labels[ri], s, dest, colors)
            plot_field(run[sindex], labels[ri], s, dest, colors, "99p")
            # plot_field(run[sindex], labels[ri], s, dest, colors, "user_cache_usage")
            seriess[s].append(val)
            errors[s].append(err_f(run[sindex]))

    x = np.arange(len(labels))

    width = 0.35
    fig, ax = plt.subplots()
    print("seriess", seriess)

    for sindex in range(len(series_labels)):
        s = series_labels[sindex]
        ax.bar(x - (width/2)/len(series_labels) + (width*sindex/len(series_labels)), seriess[s], width/len(series_labels), label=s, yerr=errors[s])

    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    # Display the plot
    plt.savefig(dest)
