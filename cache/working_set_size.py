from common import *
import json
import time
import math
import os

# CACHE_SIZE = 5 * (2**30) # isolated cache size 5 GB
# RECORD_SIZE = 2 ** 20 # 1 MB records

CACHE_SIZE = 5 * (2**26) # isolated cache size 5 GB
RECORD_SIZE = 2 ** 16 # 1 MB records
NUM = 16
STEADY_NUM = 12
RAMP_UP_NUM = 2

TARGET_RATE = 3000 # Bad clents'
BAD_WORKING_SET_SIZE = 100000
NUM_RECORDS_PER_SHARD = 64
OPERATION_TIME = 5 # multiply by ~7 for real

USE_CACHED = False
NO_LOAD_RUN = True

MILLISECOND_INTERVAL = get_args(1)["status.interval_ms"] # use default interval
WARMUP_SECONDS = 90
COOLDOWN_SECONDS = 2
filterer = lambda d: remove_outliers(d[(d['client_id'].isin([1])) & (d["op_type"] == "READ")].iloc[
    WARMUP_SECONDS*1000//MILLISECOND_INTERVAL:-COOLDOWN_SECONDS*1000//MILLISECOND_INTERVAL
    ]['avg'])

# ___ Calculated
BAD_NUM = NUM - STEADY_NUM - RAMP_UP_NUM
CACHE_SHARD_BITS_CALC = lambda size: int(math.log2(size // (RECORD_SIZE * NUM_RECORDS_PER_SHARD)))
CACHE_SHARD_BITS_POOLED = CACHE_SHARD_BITS_CALC(CACHE_SIZE * NUM)
CACHE_SHARD_BITS_ISOLATED = CACHE_SHARD_BITS_CALC(CACHE_SIZE)

OPERATION_COUNT = TARGET_RATE * NUM * OPERATION_TIME
OUTPUT_FOLDER = "./res/working_set_size"
FIELD_NUM = 16
FIELD_LENGTH = RECORD_SIZE // FIELD_NUM # 8kb records

get_target_rate = lambda s, bws=BAD_WORKING_SET_SIZE: max(20, int(TARGET_RATE*s/bws))

def do (steady_working_set_size, ramp_working_set_size, bad_working_set_size, NUM=NUM, OPERATION_COUNT = OPERATION_COUNT, load=False):
    title = f"{steady_working_set_size}_vs_{ramp_working_set_size}_vs_{bad_working_set_size}_num_{STEADY_NUM},{RAMP_UP_NUM},{BAD_NUM}_cachesize_{CACHE_SIZE//(1024*1024)}_operationcount_{OPERATION_COUNT}"
    isolation_path = f"{OUTPUT_FOLDER}/isolation_data_{title}.csv"
    fairdb_inf_path =  f"{OUTPUT_FOLDER}/fairdb_inf_data_{title}.csv"
    fairdb_100_path = f"{OUTPUT_FOLDER}/fairdb_100_{title}.csv"
    fairdb_zero_path = f"{OUTPUT_FOLDER}/fairdb_0_{title}.csv"

    if USE_CACHED:
        return pd.read_csv(isolation_path), pd.read_csv(fairdb_inf_path), pd.read_csv(fairdb_100_path), pd.read_csv(fairdb_zero_path)

    dump_args = {}
    args = get_args(NUM)
    
    args["target_rates"] = (
        [get_target_rate(steady_working_set_size, bad_working_set_size)] * STEADY_NUM + 
        [get_target_rate(ramp_working_set_size, bad_working_set_size)] * RAMP_UP_NUM +
        [get_target_rate(bad_working_set_size, bad_working_set_size)] * BAD_NUM
    )

    args["rocksdb.cache_size"] = [CACHE_SIZE] * NUM
    args["operationcount"] = OPERATION_COUNT
    args["fieldlength"] = FIELD_LENGTH
    args["recordcount"] = [bad_working_set_size] * NUM
    args["requestdistribution"] = ["uniform"] * NUM

    dump_args["load"] = {**args}
    if load: do_load(args, NUM)
    args["recordcount"] = (
        [steady_working_set_size] * STEADY_NUM + 
        [ramp_working_set_size] * RAMP_UP_NUM +
        [bad_working_set_size] * BAD_NUM
    )
    args["ramp_duration"] = (
        [0] * STEADY_NUM + 
        [40] * RAMP_UP_NUM +
        [0] * BAD_NUM
    )

    args["ramp_start"] = (
        [0] * STEADY_NUM + 
        [60] * RAMP_UP_NUM +
        [0] * BAD_NUM
    )

    # isolation
    args["fairdb_use_pooled"] = False
    args["cache_num_shard_bits"] = CACHE_SHARD_BITS_ISOLATED
    dump_args["isolation_run"] = {**args}
    isolation_data = do_run(args)

    # fairdb infinite rad
    args["fairdb_use_pooled"] = True
    args["rocksdb.cache_size"][0] *= NUM
    args["cache_num_shard_bits"] = CACHE_SHARD_BITS_POOLED
    args["fairdb_cache_rad"] = 1000000
    dump_args["fairdb_inf_rad"] = {**args}
    fairdb_inf = do_run(args)

    # fairdb 100 rad
    args["fairdb_cache_rad"] = 100
    dump_args["fairdb_hundred_rad"] = {**args}
    fairdb_hundred = do_run(args)

    # fairdb zero rad
    args["fairdb_cache_rad"] = 0
    dump_args["fairdb_zero_rad"] = {**args}
    fairdb_zero = do_run(args)

    print(f"""
    ISOLATION DATA: {filterer(isolation_data).mean()}
    {isolation_data}
    
    FAIRDB INF RAD DATA {filterer(fairdb_inf).mean()}
    {fairdb_inf}
    
    FAIRDB 100 RAD DATA {filterer(fairdb_hundred).mean()}
    {fairdb_hundred}

    FAIRDB ZERO RAD DATA {filterer(fairdb_zero).mean()}
    {fairdb_zero}
    """)
    
    with open(f"{OUTPUT_FOLDER}/args_{title}.json", "w") as f:
        json.dump(dump_args, f, indent=4)
    
    isolation_data.to_csv(isolation_path)
    fairdb_inf.to_csv(fairdb_inf_path)
    fairdb_hundred.to_csv(fairdb_100_path)
    fairdb_zero.to_csv(fairdb_zero_path)
    time.sleep(1)
    return isolation_data, fairdb_inf, fairdb_hundred, fairdb_zero

# BAD
MULTIPLIER = 0.9
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

def f(d):
    fil = filterer(d)
    print(list(fil)[:-100], f"... ({len(fil) - 100} more)")
    return fil.mean()

plot_data(labels=[''],
    series_labels=['Isolation', 'Pooled (FairDB Infinite RAD)', 'FairDB 100 RAD', 'FairDB 0 RAD'],
    data=[
        do (slightly_under_half, slightly_under_fair, BAD_WORKING_SET_SIZE, NUM, load=not NO_LOAD_RUN)
    ],
    f=f, #lambda d: filterer(d).mean(),
    err_f=lambda d: filterer(d).std(),
    x_label="Working set of the Steady-Ramp client",
    y_label="Average (Mean) Latency",
    title=f'Steady clients\' latencies (when {STEADY_NUM} Steady vs {RAMP_UP_NUM} Ramp vs {BAD_NUM} bad)',
    dest=f"{OUTPUT_FOLDER}/graph_bad_num_{NUM}_cachesize_{CACHE_SIZE//(1024*1024)}_operationcount_{OPERATION_COUNT}.png",
    colors=["grey"] * STEADY_NUM + ["blue"] * RAMP_UP_NUM + ["red"] * BAD_NUM)


# plot_data(labels=['Very small','Slightly under 1/2', 'Slightly under fair', 'Slightly Above fair'],
#     series_labels=['Isolation', 'Pooled (FairDB Infinite RAD)', 'FairDB 100 RAD', 'FairDB 0 RAD'],
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
#     dest=f"{OUTPUT_FOLDER}/graph_bad_num_{NUM}_cachesize_{CACHE_SIZE//(1024*1024)}_operationcount_{OPERATION_COUNT}.png")

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
#     dest=f"{OUTPUT_FOLDER}/graph_good_num_{NUM}_cachesize_{CACHE_SIZE//(1024*1024)}_operationcount_{OPERATION_COUNT}.png")

print(config)
print("done")
