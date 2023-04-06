import time

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor

from pubsub_datagen import SubGenSharder
from pubsub_datagen import gen_subscriptions_worker
from pubsub_datagen import Publication
from pubsub_datagen import print_sub_counts
from pubsub_datagen import combine_sub_worker_results


if __name__ == "__main__":


    import cpuinfo     # mamba install py-cpuinfo   OR   pip install py-cpuinfo

    print("Testing subscription generator.")
    print("Testing platform information:")
    for key, value in cpuinfo.get_cpu_info().items():
        if key.startswith("cpuinfo"):
            continue
        print("\t{0}: {1}".format(key, value))

    n = 1_000_000
    num_workers = 5

    field_percentages = {
        "city": 0.5,
        "temp": 0.5,
        "station_id": 0.3,
        "wind": 0.,
        "wind_direction": 0.,
        "date": 0.,
    }
    op_percentages = {
        ("city", "="): 0.5,
        ("station_id", "="): 0.5,
    }
    
    print("Parameters:")
    print(field_percentages)
    print(op_percentages)

    sharder = SubGenSharder()

    print("\nTesting MULTIPROCESSING work on num workers:", num_workers)
    start_time = time.perf_counter()
    workers = sharder.shard(
        n,
        field_percentages,
        op_percentages,
        num_workers=num_workers,
        return_params_only=False, )
    with ProcessPoolExecutor(max_workers=min(5, num_workers)) as executor:
        futures = [executor.submit(
            # gen_subscriptions_worker, sharder.fields, worker   # if return_params_only is True
            worker
        ) for worker in workers]
        results = [future.result() for future in futures]   # results per worker

    total_time = time.perf_counter() - start_time
    print("Generated", n, "subscriptions over", num_workers, "workers")
    print(f'Duration: {total_time:.3f}')
    print("Rate of subscriptions per second:", round(n / max(1, total_time), 1))
    print_sub_counts(*combine_sub_worker_results(results))   # PRINT COUNTS
    print(results[0][:2])
    print("-" * 100, end="\n\n")

    print("Testing MULTITHREADING work on num workers:", num_workers)
    start_time = time.perf_counter()
    workers = sharder.shard(
        n,
        field_percentages,
        op_percentages,
        num_workers=num_workers,
        return_params_only=False, )
    with ThreadPoolExecutor(max_workers=min(5, num_workers)) as executor:
        futures = [executor.submit(
            # gen_subscriptions_worker, sharder.fields, worker   # if return_params_only is True
            worker
        ) for worker in workers]
        results = [future.result() for future in futures]   # results per worker

    total_time = time.perf_counter() - start_time
    print("Generated", n, "subscriptions over", num_workers, "workers")
    print(f'Duration: {total_time:.3f}')
    print("Rate of subscriptions per second:", round(n / max(1, total_time), 1))
    print_sub_counts(*combine_sub_worker_results(results))   # PRINT COUNTS
    # print(results[0][:2])
    for res in results:
        for sub in res:
            print(sub)
