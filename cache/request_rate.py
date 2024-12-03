from common import *
import json

CACHE_SIZE = 10 * 1024 * 1024
OPERATION_COUNT = 1000000 # 1 mil
NUM = 4
TABLE_SIZE = 100000
OUTPUT_FOLDER = "./res/request_rate"

def do (rate, opp_rate, NUM=4, TABLE_SIZE=10000, OPERATION_COUNT = 1000000, load=False):
    dump_args = {}
    args = get_args(NUM)
    args["target_rates"] = [rate] + [opp_rate] * (NUM-1)
    args["rocksdb.cache_size"] = [CACHE_SIZE] * NUM
    args["operationcount"] = OPERATION_COUNT
    args["recordcount"] = [TABLE_SIZE] * NUM
    args["requestdistribution"] = ["uniform"] * NUM

    if load:
        dump_args["load"] = {**args}
        do_load(args, NUM)

    # isolation
    dump_args["isolation_run"] = {**args}
    isolation_data = do_run(args)

    # pooled
    args["client_to_cf_map"] = ["default"] * NUM
    args["client_to_cf_offset"] = list(range(0,NUM*TABLE_SIZE,TABLE_SIZE))
    args["rocksdb.cache_size"][0] *= NUM
    dump_args["pooled_run"] = {**args}
    pooled_data = do_run(args)

    print(f"ISOLATION DATA: {filterer(isolation_data).mean()}\n{isolation_data}\n\nPOOLED DATA {filterer(pooled_data).mean()}:\n{pooled_data}\n\n")
    
    title = f"{rate}_vs_{opp_rate}_num_{NUM}_tablesize_{TABLE_SIZE}_cachesize_{CACHE_SIZE//(1024*1024)}_operationcount_{OPERATION_COUNT}"
    with open(f"{OUTPUT_FOLDER}/args_{title}.json", "w") as f:
        json.dump(dump_args, f, indent=4)
    
    isolation_data.to_csv(f"{OUTPUT_FOLDER}/isolation_data_{title}.csv")
    pooled_data.to_csv(f"{OUTPUT_FOLDER}/pooled_data_{title}.csv")
    return isolation_data, pooled_data

# BAD
plot_data(labels=['100x', '10x', '5x', '2x'], 
    data=[
        do (50, 5000, NUM, TABLE_SIZE , load=True),
        do (500, 5000, NUM, TABLE_SIZE),
        do (1000, 5000, NUM, TABLE_SIZE),
        do (2000, 4000, NUM, TABLE_SIZE)
    ],
    f=lambda d: filterer(d).mean(),
    err_f=lambda d: filterer(d).std(),
    x_label="Request Rate of others relative to first person",
    y_label="Average (Mean) Latency",
    title='Affect of Comparative Request Rate on Latency in RocksDB Block Cache',
    dest=f"{OUTPUT_FOLDER}/graph_bad_num_{NUM}_tablesize_{TABLE_SIZE}_cachesize_{CACHE_SIZE//(1024*1024)}_operationcount_{OPERATION_COUNT}.png")

# GOOD
plot_data(labels=['100', '500', '1000'], # , 4000 
    data=[
        do (100, 100, NUM, TABLE_SIZE , 100*200, True),
        do (500, 500, NUM, TABLE_SIZE, 500*200),
        do (1000, 1000, NUM, TABLE_SIZE, 1000*200),
        do (4000, 4000, NUM, TABLE_SIZE, 4000*400)
    ],
    f=lambda d: filterer(d).mean(),
    err_f=lambda d: filterer(d).std(),
    x_label="Request Rate of each client (Requests per second)",
    y_label="Average (Mean) Latency",
    title='Affect of Request Rate on Latency in RocksDB Block Cache',
    dest=f"{OUTPUT_FOLDER}/graph_good_num_{NUM}_tablesize_{TABLE_SIZE}_cachesize_{CACHE_SIZE//(1024*1024)}_operationcount_{OPERATION_COUNT}.png")

print("done")
