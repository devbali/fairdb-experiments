from cache.common import get_args, do_load, do_run
import sys

NUM = 4
OPERATION_COUNT = 1000000
FIELD_LENGTH = 1024
CACHE_SIZE = 1024*1024
TARGET_RATE = 1000
args = get_args(NUM)

args["operationcount"] = OPERATION_COUNT
args["fieldlength"] = FIELD_LENGTH
args["rocksdb.cache_size"] = [CACHE_SIZE] * NUM
args["target_rates"] = [TARGET_RATE] * NUM
args["fairdb_use_pooled"] = True

if len(sys.argv) > 1:
    do_load(args, NUM)

do_run(args, output=True)
