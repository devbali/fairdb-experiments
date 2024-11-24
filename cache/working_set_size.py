from common import *
import json

CACHE_SIZE = 10 * 1024 * 1024
OPERATION_COUNT = 500000
FIELD_LENGTH = 512 # 8kb records
FIELD_NUM = 16
NUM = 4
OUTPUT_FOLDER = "./res/working_set_size"
TARGET_RATE = 1000
USE_CACHED = True

RECORD_SIZE = FIELD_LENGTH * FIELD_NUM

def do (working_set_size, opp_working_set_size, NUM=4, OPERATION_COUNT = OPERATION_COUNT, load=False):
    title = f"{working_set_size}_vs_{opp_working_set_size}_num_{NUM}_cachesize_{CACHE_SIZE//(1024*1024)}_operationcount_{OPERATION_COUNT}"
    isolation_path = f"{OUTPUT_FOLDER}/isolation_data_{title}.csv"
    pooled_path = f"{OUTPUT_FOLDER}/pooled_data_{title}.csv"
    fairdb_pooled_path =  f"{OUTPUT_FOLDER}/fairdb_pooled_data_{title}.csv"
    fairdb_half_path = f"{OUTPUT_FOLDER}/fairdb_pooled_half_{title}.csv"

    if USE_CACHED:
        print("reading from cached", isolation_path, pooled_path, "vals", len(pd.read_csv(isolation_path).query("client_id == 0")), len(filterer(pd.read_csv(isolation_path))))
        return pd.read_csv(isolation_path), pd.read_csv(pooled_path), pd.read_csv(fairdb_pooled_path), pd.read_csv(fairdb_half_path)

    dump_args = {}
    args = get_args(NUM)
    args["target_rates"] = [max(20, int(TARGET_RATE*working_set_size/opp_working_set_size))] + [TARGET_RATE] * (NUM-1)
    args["rocksdb.cache_size"] = [CACHE_SIZE] * NUM
    args["operationcount"] = OPERATION_COUNT
    args["fieldlength"] = FIELD_LENGTH
    args["recordcount"] = [opp_working_set_size] * NUM
    args["requestdistribution"] = ["uniform"] * NUM

    dump_args["load"] = {**args}
    do_load(args, NUM)
    args["recordcount"] = [working_set_size] + [opp_working_set_size] * (NUM-1)

    # isolation
    dump_args["isolation_run"] = {**args}
    isolation_data = do_run(args)

    # fairdb pooled
    args["fairdb_use_pooled"] = True
    dump_args["fairdb_pooled_run"] = {**args}
    fairdb_pooled = do_run(args)

    # fairdb half
    args["fairdb_reserved_space"] = CACHE_SIZE // 2
    dump_args["fairdb_half_run"] = {**args}
    fairdb_half = do_run(args)

    # pooled load
    pool_load_args = get_args(4)
    pool_load_args["recordcount"] = [opp_working_set_size*NUM]
    pool_load_args["fieldlength"] = FIELD_LENGTH
    do_load(pool_load_args, 1)

    # pooled
    args["fairdb_use_pooled"] = False
    args["client_to_cf_map"] = ["default"] * NUM
    args["client_to_cf_offset"] = list(range(0,NUM*opp_working_set_size,opp_working_set_size))
    args["rocksdb.cache_size"][0] *= NUM
    dump_args["pooled_run"] = {**args}
    pooled_data = do_run(args)

    print(f"""
    ISOLATION DATA: {filterer(isolation_data).mean()}
    {isolation_data}
    
    POOLED DATA {filterer(pooled_data).mean()}:
    {pooled_data}
    
    FAIRDB POOLED DATA {filterer(fairdb_pooled).mean()}
    {fairdb_pooled}

    FAIRDB HALF RESERVED DATA {filterer(fairdb_half).mean()}
    {fairdb_half}
    """)
    
    with open(f"{OUTPUT_FOLDER}/args_{title}.json", "w") as f:
        json.dump(dump_args, f, indent=4)
    
    isolation_data.to_csv(isolation_path)
    pooled_data.to_csv(pooled_path)
    fairdb_pooled.to_csv(fairdb_pooled_path)
    fairdb_half.to_csv(fairdb_half_path)
    return isolation_data, pooled_data, fairdb_pooled, fairdb_half

# BAD
plot_data(labels=['Very small','Slightly under 1/2', 'Slightly under fair', 'Slightly Above fair'],
    series_labels=['Isolation', 'Pooled (single cf)', 'FairDB Pooled', 'FairDB w/ 1/2 reserved'],
    data=[
        do (100, 100000, NUM, load=True ),
        do (int (0.8* ((CACHE_SIZE/2) / RECORD_SIZE)), 100000, NUM),
        do (int (0.8* ((CACHE_SIZE) / RECORD_SIZE)), 100000, NUM),
        do (CACHE_SIZE // RECORD_SIZE, 100000, NUM),
    ],
    f=lambda d: filterer(d).mean(),
    err_f=lambda d: filterer(d).std(),
    x_label="Working set of the Good client",
    y_label="Average (Mean) Latency",
    title='Good (small working set) vs Bad (large working set)',
    dest=f"{OUTPUT_FOLDER}/graph_bad_num_{NUM}_cachesize_{CACHE_SIZE//(1024*1024)}_operationcount_{OPERATION_COUNT}.png")

# GOOD
# plot_data(labels=['100', '500', '1000'], # , 4000 
#     data=[
#         do (100, 100, NUM , 100*200, True),
#         do (500, 500, NUM, 500*200),
#         do (1000, 1000, NUM, 1000*200),
#         #do (4000, 4000, NUM, 4000*400)
#     ],
#     f=lambda d: filterer(d).mean(),
#     err_f=lambda d: filterer(d).std(),
#     x_label="Request Rate of each client (Requests per second)",
#     y_label="Average (Mean) Latency",
#     title='Affect of Request Rate on Latency in RocksDB Block Cache',
#     dest=f"{OUTPUT_FOLDER}/graph_good_num_{NUM}_cachesize_{CACHE_SIZE//(1024*1024)}_operationcount_{OPERATION_COUNT}.png")

print("done")
