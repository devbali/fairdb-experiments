from common import *
import json
import time
import math
import os
import sys

# 4 kb records
RECORD_SIZE = 4 * 2 ** 10

SMALL_RUN = "small" in sys.argv[1]
DISTRIBUTION = "zipfian" if "zipfian" in sys.argv[1] else "uniform"
MULTI_RUN = "multi" in sys.argv[1]
RAMP_DOWN = "rampdown" in sys.argv[1]

SMALL_SCALE_FACTOR = 16

# isolated cache size 5 GB
CACHE_SIZE = 5 * 2 ** 30
if SMALL_RUN:
    CACHE_SIZE  //= SMALL_SCALE_FACTOR

# Working set size for bad clients is 100 GB
BAD_WORKING_SET_SIZE = (100 * 2 ** 30) // RECORD_SIZE
if SMALL_RUN:
    BAD_WORKING_SET_SIZE //= SMALL_SCALE_FACTOR


# 12 Steady, 2 Ramp down and up, 2 Bad clients, for a total of 16
NUM = 16
STEADY_NUM = 12
RAMP_UP_NUM = 2
BAD_NUM = NUM - STEADY_NUM - RAMP_UP_NUM  # 2

# We target a read io throughput, base the bad clients' target rate based on that
# Other clients' rates will be scaled accordingly so throughput is at TARGET_BANDWIDTH
REAL_IO_BANDWIDTH = 6000 * 2 ** 20
TARGET_BANDWIDTH = REAL_IO_BANDWIDTH // 16
TARGET_RATE = int((TARGET_BANDWIDTH / RECORD_SIZE) / 2.405) # Bad clents' only (~2.405x for total)

NUM_TPOOL_THREADS = 360
NUM_RECORDS_PER_SHARD = 256

RADS = [math.ceil(10**6* ratio * CACHE_SIZE / (TARGET_BANDWIDTH / NUM)) for ratio in [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]]
WARMUP_SECONDS = int(BAD_WORKING_SET_SIZE/TARGET_RATE)

if MULTI_RUN:
    NUM_READ_BURST_CYCLES = 10 if not SMALL_RUN else 3
    RAMP_START = 0
    RAMP_DURATION = 0
    BAD_RAMP_START = 0
    BAD_RAMP_DURATION = 0
    OPERATION_TIME = 300 if not SMALL_RUN else 60

    # 12 mins (warmup) * 7 + 2 * 5 mins + 5 * 50 mins =  6 hrs

elif RAMP_DOWN:
    NUM_READ_BURST_CYCLES = 0
    RAMP_START = 0 # FRACTION OUT OF 120, not time
    RAMP_DURATION = 0 # FRACTION OUT OF 120, not time
    BAD_RAMP_START = 6
    BAD_RAMP_DURATION = 120-BAD_RAMP_START
    OPERATION_TIME = 1200 if not SMALL_RUN else 900

    # 12 mins (warmup) * 6 + 6 * 60 mins =  432 mins ~ 7-8 hrs

else: # stable
    NUM_READ_BURST_CYCLES = 0
    RAMP_START = 0 # FRACTION OUT OF 120, not time
    RAMP_DURATION = 0 # FRACTION OUT OF 120, not time
    BAD_RAMP_START = 0
    BAD_RAMP_DURATION = 0
    STEADY_NUM = 14
    RAMP_UP_NUM = 0
    RADS = [RADS[0]] + RADS[2:]
    OPERATION_TIME = 600 if not SMALL_RUN else 40
    WARMUP_SECONDS += 30 if not SMALL_RUN else 10

    # 12 mins (warmup) * 6 + 10 * 6 = 2.5 hrs

MILLISECOND_INTERVAL = get_args(1)["status.interval_ms"] # use default interval
COOLDOWN_SECONDS = 2

USE_CACHED = "cached" in sys.argv[1]
NO_LOAD_RUN = True

# ___ Calculated
CACHE_SHARD_BITS_CALC = lambda size: int(math.log2(size // (RECORD_SIZE * NUM_RECORDS_PER_SHARD)))
CACHE_SHARD_BITS_POOLED = CACHE_SHARD_BITS_CALC(CACHE_SIZE * NUM)
CACHE_SHARD_BITS_ISOLATED = CACHE_SHARD_BITS_CALC(CACHE_SIZE)

OUTPUT_FOLDER = "./res/working_set_size"
FIELD_NUM = 16
FIELD_LENGTH = RECORD_SIZE // FIELD_NUM # 8kb records

get_target_rate = lambda s, bws=BAD_WORKING_SET_SIZE: max(20, int(TARGET_RATE*s/bws))

def get_read_burst_num_records (RAD):
    TOTAL_THROUGHPUT = 348 # 6000 # in mb/s
    READ_BURST_SIZE = (RAD/10**6) * (TOTAL_THROUGHPUT * 2**20 / NUM)
    READ_BURST_NUM_RECORDS = int(READ_BURST_SIZE // RECORD_SIZE)
    print(f"RAD {RAD/10**6} seconds. READ BURST NUM RECORDS: {READ_BURST_NUM_RECORDS}. Size: {READ_BURST_SIZE / 2**20} MB = {READ_BURST_SIZE / 2**30} GB")
    return READ_BURST_NUM_RECORDS


def get_with_throughput (csv_path_or_df, ramp_working_set_size):
    if isinstance(csv_path_or_df, str):
        df = pd.read_csv(csv_path_or_df)
    else: df = csv_path_or_df
    df["time_s"] = (df["timestamp"] - df["timestamp"].min()) / 1000
    def get_throughput_for_row (r):
        assert r["op_type"] in ["READ", "MULTI_READ", "READ-FAILED", "MULTI_READ-FAILED"]
        if r["op_type"] == "READ":
            return RECORD_SIZE * r["count"]
        if r["op_type"] == "MULTI_READ":
            return RECORD_SIZE * r["count"] * ramp_working_set_size
    df["throughput"] = df.apply(get_throughput_for_row, axis=1)
    return df

def do (steady_working_set_size, ramp_working_set_size, bad_working_set_size, NUM=NUM, OPERATION_TIME = OPERATION_TIME, RADS=RADS, load=False):
    title = f"{steady_working_set_size}_vs_{ramp_working_set_size}_vs_{bad_working_set_size}_num_{STEADY_NUM},{RAMP_UP_NUM},{BAD_NUM}_cachesize_{CACHE_SIZE//(1024*1024)}_operationtime_{OPERATION_TIME}"
    isolation_path = f"{OUTPUT_FOLDER}/isolation_data_{title}.csv"
    fairdb_path = lambda rad: f"{OUTPUT_FOLDER}/fairdb_{rad}_{title}.csv"

    if USE_CACHED:
        return [get_with_throughput(isolation_path,ramp_working_set_size), *[get_with_throughput(fairdb_path(r),ramp_working_set_size) for r in RADS]]

    dump_args = {}
    args = get_args(NUM)
    
    args["target_rates"] = (
        [get_target_rate(steady_working_set_size, bad_working_set_size)] * STEADY_NUM + 
        [get_target_rate(ramp_working_set_size, bad_working_set_size)] * RAMP_UP_NUM +
        [get_target_rate(bad_working_set_size, bad_working_set_size)] * BAD_NUM
    )

    args["rocksdb.cache_size"] = [CACHE_SIZE] * NUM
    args["operationcount"] = sum([rate * OPERATION_TIME for rate in args["target_rates"]])
    args["fieldlength"] = FIELD_LENGTH
    args["recordcount"] = [bad_working_set_size] * NUM
    args["requestdistribution"] = [DISTRIBUTION] * NUM
    args["forced_warmup"] = True
    args["tpool_threads"] = NUM_TPOOL_THREADS

    dump_args["load"] = {**args}
    if load: do_load(args, NUM)
    args["recordcount"] = (
        [steady_working_set_size] * STEADY_NUM + 
        [ramp_working_set_size] * RAMP_UP_NUM +
        [bad_working_set_size] * BAD_NUM
    )
    args["ramp_duration"] = (
        [0] * STEADY_NUM + 
        [RAMP_DURATION] * RAMP_UP_NUM +
        [BAD_RAMP_DURATION] * BAD_NUM
    )

    args["ramp_start"] = (
        [0] * STEADY_NUM + 
        [RAMP_START] * RAMP_UP_NUM +
        [BAD_RAMP_START] * BAD_NUM
    )

    # isolation
    args["fairdb_use_pooled"] = False
    args["cache_num_shard_bits"] = CACHE_SHARD_BITS_ISOLATED
    dump_args["isolation_run"] = {**args}
    USE_CACHED_FOR_ISOLATED = False
    USE_CACHED_FOR_ZERO_RAD = False

    if USE_CACHED_FOR_ISOLATED:
        isolation_data = get_with_throughput(isolation_path,ramp_working_set_size)
    else:
        isolation_data = get_with_throughput(do_run(args),ramp_working_set_size)
        isolation_data.to_csv(isolation_path)

    print(f"""
    ISOLATION DATA: {filterer(isolation_data).mean()}
    {isolation_data}""")

    # fairdb infinite rad
    args["fairdb_use_pooled"] = True
    args["rocksdb.cache_size"][0] *= NUM
    args["cache_num_shard_bits"] = CACHE_SHARD_BITS_POOLED

    datas = []
    for rad in RADS:
        args["num_read_burst_cycles"] = [NUM_READ_BURST_CYCLES if rad > 0 else 0] * NUM
        args["fairdb_cache_rad"] = rad
        args["read_burst_num_records"] = [0] * STEADY_NUM + [ramp_working_set_size] * RAMP_UP_NUM + [0] * BAD_NUM
        dump_args[f"fairdb_{rad}_rad"] = {**args}
        if rad == 0 and USE_CACHED_FOR_ZERO_RAD: datas.append(get_with_throughput(fairdb_path(rad),ramp_working_set_size))
        else:
            datas.append(get_with_throughput(do_run(args), ramp_working_set_size))
            datas[-1].to_csv(fairdb_path(rad))

        print(f"""
    FAIRDB RAD {rad//10**3} ms {filterer(datas[-1]).mean()}
    {datas[-1]}
""")
    
    with open(f"{OUTPUT_FOLDER}/args_{title}.json", "w") as f:
        json.dump(dump_args, f, indent=4)

    time.sleep(1)
    return [isolation_data, *datas]

# BAD
MULTIPLIER = 0.8
slightly_under_half = int (MULTIPLIER* ((CACHE_SIZE/2) / RECORD_SIZE))
slightly_under_fair = int (MULTIPLIER* ((CACHE_SIZE) / RECORD_SIZE))
slightly_above_fair = CACHE_SIZE // RECORD_SIZE

config = f"""
Cache size fair share in GB: {CACHE_SIZE/(2**30) : .4} (Total {NUM*CACHE_SIZE/(2**30) : .4})
Dataset size per client in GB: {BAD_WORKING_SET_SIZE*RECORD_SIZE/(2**30) : .4} (Total {NUM*BAD_WORKING_SET_SIZE*RECORD_SIZE/(2**30) : .4})
Pooled shard {CACHE_SHARD_BITS_POOLED} bits, {2**CACHE_SHARD_BITS_POOLED} shards total")
If 16 Bad, Read io bandwidth is targetted at {TARGET_RATE*RECORD_SIZE / (2**20) : .9} MB/s
Records are {RECORD_SIZE//(2**10)} KB each
Working set sizes:
    Bad: {BAD_WORKING_SET_SIZE}
    Ramp: {slightly_under_fair} (slightly under fair share)
    Steady: {slightly_under_half} (slightly under half of fair share)

Target Rates:
    Bad: {TARGET_RATE} req/s
    Ramp: {get_target_rate(slightly_under_fair)} req/s
    Steady: {get_target_rate(slightly_under_half)} req/s

* Each record is equally likely to be requested in any given time period *
"""
print(config)

def f(i,d):
    df = d[(d['client_id'].isin([i])) & (d["op_type"] == "READ")]
    max_time = df["time_s"].max()
    d = df[(df["time_s"] > WARMUP_SECONDS) & (df["time_s"] < max_time - COOLDOWN_SECONDS)]
    print(d)
    return d

labels = ["Isolation"]
for rad in RADS:
    labels.append(f"{rad//10**3} ms RAD")
labels[-1] = 'Pooled'

plot_data(labels=[''],
    series_labels=labels,
    data=[
        do (slightly_under_half, slightly_under_fair, BAD_WORKING_SET_SIZE, NUM, load=not NO_LOAD_RUN)
    ],
    f=f, #lambda d: filterer(d).mean(),
    x_label="Working set of the Steady-Ramp client",
    y_label="Average (Mean) Latency",
    title=f'Steady clients\' latencies (when {STEADY_NUM} Steady vs {RAMP_UP_NUM} Ramp vs {BAD_NUM} bad)',
    dest=f"{OUTPUT_FOLDER}/num_{NUM}_cachesize_{CACHE_SIZE//(2**30)}_time_{OPERATION_TIME}.png",
    colors=["grey"] * STEADY_NUM + ["blue"] * RAMP_UP_NUM + ["red"] * BAD_NUM,
    rads = [0] + RADS, warmup_seconds=WARMUP_SECONDS)

if len(sys.argv) > 2:
    arch_path = f"./res_archives/{sys.argv[2]}/{sys.argv[1]}"
    os.system(f"mkdir -p {arch_path}; mv {OUTPUT_FOLDER}/* {arch_path}")

# plot_data(labels=['Very small','Slightly under 1/2', 'Slightly under fair', 'Slightly Above fair'],
#     series_labels=['Isolation', 'Pooled (FairDB Infinite RAD)', 'FairDB 5000000 RAD', 'FairDB 0 RAD'],
#     data=[
#         do (slightly_under_half//4, BAD_WORKING_SET_SIZE, NUM, load=not NO_LOAD_RUN),
#         do (slightly_under_half, BAD_WORKING_SET_SIZE, NUM),
#         do (slightly_under_fair, BAD_WORKING_SET_SIZE, NUM),
#         do (slightly_above_fair, BAD_WORKING_SET_SIZE, NUM),
#     ],
#     f=lambda d: filterer(d).mean(),
#     err_f=lambda d: filterer(d).std(),
#     x_label="Working set of the Good client",
#     y_label="Average (Mean) Latency",
#     title=f'Good clients\' latencies (when {GOOD_NUM} Good vs {NUM-GOOD_NUM} Bad)',
#     dest=f"{OUTPUT_FOLDER}/graph_bad_num_{NUM}_cachesize_{CACHE_SIZE//(1024*1024)}_operationtime_{OPERATION_TIME}.png")

# # GOOD

# plot_data(labels=['Very small','Slightly under 1/2', 'Slightly under fair', 'Slightly Above fair'],
#     series_labels=['Isolation', 'Pooled (single cf)', 'FairDB Pooled', 'FairDB w/ 1/2 reserved'],
#     data=[
#         do (100, 100, NUM),
#         do (slightly_under_half, slightly_under_half, NUM),
#         do (slightly_under_fair, slightly_under_fair, NUM),
#         do (slightly_above_fair, slightly_above_fair, NUM),
#     ],
#     f=lambda d: filterer(d).mean(),
#     err_f=lambda d: filterer(d).std(),
#     x_label="Working set of the Good client",
#     y_label="Average (Mean) Latency",
#     title='4 of the same client',
#     dest=f"{OUTPUT_FOLDER}/graph_good_num_{NUM}_cachesize_{CACHE_SIZE//(1024*1024)}_operationtime_{OPERATION_TIME}.png")

print(config)
print("done")
