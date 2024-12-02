from common import *
import json
import time
import math

CACHE_SIZE = 10 * 1024 * 1024 # isolated cache size
OPERATION_COUNT = 500000
FIELD_LENGTH = 512 # 8kb records
FIELD_NUM = 16
NUM = 16
OUTPUT_FOLDER = "./res/working_set_size"
TARGET_RATE = 2000
GOOD_NUM = 14
USE_CACHED = False

RECORD_SIZE = FIELD_LENGTH * FIELD_NUM

NUM_RECORDS_PER_SHARD_IN_POOLED = 128
CACHE_SHARD_BITS = math.log2((CACHE_SIZE * NUM) // (RECORD_SIZE * NUM_RECORDS_PER_SHARD_IN_POOLED))


def do (working_set_size, opp_working_set_size, NUM=NUM, OPERATION_COUNT = OPERATION_COUNT, load=False):
    title = f"{working_set_size}_vs_{opp_working_set_size}_num_{NUM}_cachesize_{CACHE_SIZE//(1024*1024)}_operationcount_{OPERATION_COUNT}"
    isolation_path = f"{OUTPUT_FOLDER}/isolation_data_{title}.csv"
    pooled_path = f"{OUTPUT_FOLDER}/pooled_data_{title}.csv"
    fairdb_inf_path =  f"{OUTPUT_FOLDER}/fairdb_inf_data_{title}.csv"
    fairdb_100_path = f"{OUTPUT_FOLDER}/fairdb_100_{title}.csv"
    fairdb_zero_path = f"{OUTPUT_FOLDER}/fairdb_0_{title}.csv"

    if USE_CACHED:
        print("reading from cached", isolation_path, pooled_path, "vals", len(pd.read_csv(isolation_path).query("client_id == 0")), len(filterer(pd.read_csv(isolation_path))))
        return pd.read_csv(isolation_path), pd.read_csv(pooled_path), pd.read_csv(fairdb_pooled_path), pd.read_csv(fairdb_half_path)

    dump_args = {}
    args = get_args(NUM)
    args["target_rates"] = [max(20, int(TARGET_RATE*working_set_size/opp_working_set_size))] * GOOD_NUM + [TARGET_RATE] * (NUM-GOOD_NUM)
    args["rocksdb.cache_size"] = [CACHE_SIZE] * NUM
    args["operationcount"] = OPERATION_COUNT
    args["fieldlength"] = FIELD_LENGTH
    args["recordcount"] = [opp_working_set_size] * NUM
    args["requestdistribution"] = ["uniform"] * NUM

    dump_args["load"] = {**args}
    do_load(args, NUM)
    args["recordcount"] = [working_set_size] * GOOD_NUM + [opp_working_set_size] * (NUM-GOOD_NUM)

    # isolation
    dump_args["isolation_run"] = {**args}
    isolation_data = do_run(args)

    # fairdb infinite rad
    args["fairdb_use_pooled"] = True
    args["rocksdb.cache_size"][0] *= NUM
    args["cache_num_shard_bits"] = CACHE_SHARD_BITS
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

    # pooled load
    pool_load_args = get_args(4)
    pool_load_args["recordcount"] = [opp_working_set_size*NUM]
    pool_load_args["fieldlength"] = FIELD_LENGTH
    do_load(pool_load_args, 1)

    # pooled
    args["fairdb_use_pooled"] = False
    args["client_to_cf_map"] = ["default"] * NUM
    args["client_to_cf_offset"] = list(range(0,NUM*opp_working_set_size,opp_working_set_size))
    dump_args["pooled_run"] = {**args}
    pooled_data = do_run(args)

    print(f"""
    ISOLATION DATA: {filterer(isolation_data).mean()}
    {isolation_data}
    
    POOLED DATA {filterer(pooled_data).mean()}:
    {pooled_data}
    
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
    pooled_data.to_csv(pooled_path)
    fairdb_inf.to_csv(fairdb_inf_path)
    fairdb_hundred.to_csv(fairdb_100_path)
    fairdb_zero.to_csv(fairdb_zero_path)
    time.sleep(1)
    return isolation_data, pooled_data, fairdb_inf, fairdb_hundred, fairdb_zero

# BAD
slightly_under_half = int (0.7* ((CACHE_SIZE/2) / RECORD_SIZE))
slightly_under_fair = int (0.7* ((CACHE_SIZE) / RECORD_SIZE))
slightly_above_fair = CACHE_SIZE // RECORD_SIZE

plot_data(labels=['Very small','Slightly under 1/2', 'Slightly under fair', 'Slightly Above fair'],
    series_labels=['Isolation', 'Pooled (single cf)', 'FairDB Infinite RAD', 'FairDB 100 RAD', 'FairDB 0 RAD'],
    data=[
        do (100, 100000, NUM, load=True ),
        do (slightly_under_half, 100000, NUM),
        do (slightly_under_fair, 100000, NUM),
        do (slightly_above_fair, 100000, NUM),
    ],
    f=lambda d: filterer(d).mean(),
    err_f=lambda d: filterer(d).std(),
    x_label="Working set of the Good client",
    y_label="Average (Mean) Latency",
    title='Good clients\' latencies (2 Good vs 2 Bad)',
    dest=f"{OUTPUT_FOLDER}/graph_bad_num_{NUM}_cachesize_{CACHE_SIZE//(1024*1024)}_operationcount_{OPERATION_COUNT}.png")

# GOOD

plot_data(labels=['Very small','Slightly under 1/2', 'Slightly under fair', 'Slightly Above fair'],
    series_labels=['Isolation', 'Pooled (single cf)', 'FairDB Pooled', 'FairDB w/ 1/2 reserved'],
    data=[
        do (100, 100, NUM, load=True ),
        do (slightly_under_half, slightly_under_half, NUM),
        do (slightly_under_fair, slightly_under_fair, NUM),
        do (slightly_above_fair, slightly_above_fair, NUM),
    ],
    f=lambda d: filterer(d).mean(),
    err_f=lambda d: filterer(d).std(),
    x_label="Working set of the Good client",
    y_label="Average (Mean) Latency",
    title='4 of the same client',
    dest=f"{OUTPUT_FOLDER}/graph_good_num_{NUM}_cachesize_{CACHE_SIZE//(1024*1024)}_operationcount_{OPERATION_COUNT}.png")

print("done")
