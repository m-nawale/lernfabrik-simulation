# learning_factory/stations.py
import simpy

def run_serial_station(env, name, input_store, output_store, cycle_time_s, workers_required, metrics):
    """
    Pull 1 item from input_store, process for cycle_time_s, push to output_store.
    Tracks simple utilization & output count.
    """
    busy = 0.0
    last_start = None
    produced = 0

    while True:
        # wait for an item to be available
        item = yield input_store.get()

        # start processing
        last_start = env.now
        yield env.timeout(cycle_time_s/60.0)  # convert to seconds
        busy += (env.now - last_start)

        # push to next buffer
        yield output_store.put(item)
        produced += 1

        # update metrics
        metrics.setdefault("station_output", {})[name] = produced
        metrics.setdefault("station_busy_time", {})[name] = busy
