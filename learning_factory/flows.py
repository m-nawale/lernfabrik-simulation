import random
import itertools

def new_orders_source(env, cfg, buffers, metrics):
    """
    Generate 'new' items into neu_lager using a Poisson process.
    Poisson arrivals => exponential interarrival times with mean 1/rate.
    """
    rate = cfg["arrivals"]["new_orders"]["rate_per_min"]
    counter = itertools.count(1)

    while True:
        # draw next interarrival time (minutes)
        inter = random.expovariate(rate) if rate > 0 else 10**9
        yield env.timeout(inter)

        token = {
            "id": f"NEW-{next(counter):05d}",
            "type": "new",
            "t_created": env.now,
        }

        # try to put into neu_lager (respect capacity)
        neu = buffers["neu_lager"]
        if len(neu.items) < neu.capacity:
            yield neu.put(token)
            metrics["arrivals_new"] += 1
        else:
            metrics["lost_new_due_to_neu_lager_full"] += 1

def returns_source(env, cfg, buffers, metrics):
    import random
    inter = cfg["arrivals"]["returns"]["interarrival_min"]
    batch_mean = cfg["arrivals"]["returns"]["batch_mean"]
    i = 0
    while True:
        yield env.timeout(random.expovariate(1.0 / inter))  # wait for next truck
        batch_size = max(1, int(random.gauss(batch_mean, 1)))  # ~5 ±1
        for _ in range(batch_size):
            i += 1
            token = f"RET-{i:05d}"
            yield buffers["warenannahme"].put(token)
            metrics["arrivals_returns"] += 1
