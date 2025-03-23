from common import *
import json

CACHE_SIZE = 10 * 1024 * 1024
OPERATION_COUNT = 100000
NUM = 4
TABLE_SIZE = 100000
TARGET_RATE = 1000

OUTPUT_FOLDER = "./res/distribution"

def do (zipfian_constant, NUM=4, TABLE_SIZE=10000, allzipf=False):
    dump_args = {}
    args = get_args(NUM)
    args["target_rates"] = [TARGET_RATE] * NUM
    args["rocksdb.cache_size"] = [CACHE_SIZE] * NUM
    args["operationcount"] = OPERATION_COUNT
    args["recordcount"] = [TABLE_SIZE] * (NUM)
    args["requestdistribution"] = ["zipfian"] + ["uniform"] * (NUM-1) if not allzipf else ["zipfian"] * NUM

    dump_args["load"] = {**args}
    do_load(args, NUM)

    # isolation
    args["zipfian_const"] = zipfian_constant
    dump_args["isolation_run"] = {**args}
    isolation_data = do_run(args)

    # pooled load
    pool_load_args = get_args(1)
    pool_load_args["recordcount"] = [TABLE_SIZE*NUM]
    do_load(pool_load_args, 1)

    # pooled
    args["client_to_cf_map"] = [f"default"] * NUM
    args["client_to_cf_offset"] = list(range(0,NUM*TABLE_SIZE,TABLE_SIZE))
    args["rocksdb.cache_size"][0] *= NUM
    dump_args["pooled_run"] = {**args}
    pooled_data = do_run(args)

    print(f"ISOLATION DATA: {filterer(isolation_data).mean()}\n{isolation_data}\n\nPOOLED DATA {filterer(pooled_data).mean()}:\n{pooled_data}\n\n")
    
    title = f"zipfconst_{zipfian_constant}_allzipf_{allzipf}_num_{NUM}_tablesize_{TABLE_SIZE}_cachesize_{CACHE_SIZE//(1024*1024)}_operationcount_{OPERATION_COUNT}"
    with open(f"{OUTPUT_FOLDER}/args_{title}.json", "w") as f:
        json.dump(dump_args, f, indent=4)
    
    isolation_data.to_csv(f"{OUTPUT_FOLDER}/isolation_data_{title}.csv")
    pooled_data.to_csv(f"{OUTPUT_FOLDER}/pooled_data_{title}.csv")
    return isolation_data, pooled_data

# BAD
plot_data(labels=["1.1", "1.2", "1.5", "2.0"], 
    data=[
        do ("1.1", NUM, TABLE_SIZE),
        do ("1.2", NUM, TABLE_SIZE),
        do ("1.5", NUM, TABLE_SIZE),
        do ("2.0", NUM, TABLE_SIZE)
    ],
    f=lambda d: filterer(d).mean(),
    err_f=lambda d: filterer(d).std(),
    x_label="Zipfian constant of each client",
    y_label="Average (Mean) Latency",
    title='Affect of Skew in RocksDB Block Cache',
    dest=f"{OUTPUT_FOLDER}/graph_bad_num_{NUM}_tablesize_{TABLE_SIZE}_cachesize_{CACHE_SIZE//(1024*1024)}_operationcount_{OPERATION_COUNT}.png")

# GOOD
plot_data(labels=["1.1", "1.2", "1.5", "2.0"], 
    data=[
        do ("1.1", NUM, TABLE_SIZE, True),
        do ("1.2", NUM, TABLE_SIZE, True),
        do ("1.5", NUM, TABLE_SIZE, True),
        do ("2.0", NUM, TABLE_SIZE, True)
    ],
    f=lambda d: filterer(d).mean(),
    err_f=lambda d: filterer(d).std(),
    x_label="Zipfian constant of the first person client",
    y_label="Average (Mean) Latency",
    title='Affect of Skew in RocksDB Block Cache (1 Zipfian sampling client and 3 Uniformly sampling client)',
    dest=f"{OUTPUT_FOLDER}/graph_good_num_{NUM}_tablesize_{TABLE_SIZE}_cachesize_{CACHE_SIZE//(1024*1024)}_operationcount_{OPERATION_COUNT}.png")

print("done")
