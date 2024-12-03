from cache.common import get_args, do_load, do_run
import sys

NUM = 16
OPERATION_COUNT = 16 * 1000 * 12
FIELD_LENGTH = 64
CACHE_SIZE = 1024*1024*160 # 160 mb pooled
TARGET_RATE = 1000
THREAD_COUNT = 50
args = get_args(NUM)

args["operationcount"] = OPERATION_COUNT
args["fieldlength"] = FIELD_LENGTH
args["rocksdb.cache_size"] = [CACHE_SIZE] * NUM
args["cache_num_shard_bits"] = 7
args["target_rates"] = [TARGET_RATE] * NUM
args["fairdb_use_pooled"] = False
args["fairdb_cache_rad"] = 0
args["tpool_threads"] = THREAD_COUNT
args["rsched"] = False

if len(sys.argv) > 1:
    do_load(args, NUM)

do_run(args, output=True)
