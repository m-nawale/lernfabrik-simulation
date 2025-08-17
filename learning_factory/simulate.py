from typing import Dict, Any
import random
import simpy
import pandas as pd
from learning_factory.flows import new_orders_source, returns_source
from learning_factory.stations import run_serial_station

def step_cfg(cfg, step_id: str):
    return next(s for s in cfg["forward_flow"] if s["id"] == step_id)

def run_simulation(cfg: Dict[str, Any]) -> Dict[str, Any]:
    random.seed(cfg["meta"].get("seed", 42))
    env = simpy.Environment()

    # Buffers from YAML
    buffers = {
        name: simpy.Store(env, capacity=spec["capacity"])
        for name, spec in cfg["buffers"].items()
    }

    # Metrics
    metrics = {
        "arrivals_new": 0,
        "lost_new_due_to_neu_lager_full": 0,
        "arrivals_returns": 0,
        "station_output": {},
        "station_busy_time": {},
    }

    # Arrivals
    env.process(new_orders_source(env, cfg, buffers, metrics))
    env.process(returns_source(env, cfg, buffers, metrics))

    # --- First real station: pressen_1 ---
    # Pulls from neu_lager for now (we'll add reman merge policy later)
    pressen_1_cfg = next(s for s in cfg["forward_flow"] if s["id"] == "pressen_1")
    env.process(run_serial_station(
        env,
        name="pressen_1",
        input_store=buffers["neu_lager"],
        output_store=buffers["after_pressen_1"],
        cycle_time_s=pressen_1_cfg["cycle_time_s"],
        workers_required=pressen_1_cfg.get("workers_required", 0),
        metrics=metrics,
    ))

    # --- Second station: pressen_2 ---
    p2 = step_cfg(cfg, "pressen_2")
    env.process(run_serial_station(
        env,
        name="pressen_2",
        input_store=buffers["after_pressen_1"],   # consume output of pressen_1
        output_store=buffers["after_pressen_2"],  # TEMP buffer for visibility
        cycle_time_s=p2["cycle_time_s"],          # seconds in YAML; we divide by 60 in the station
        workers_required=p2.get("workers_required", 0),
        metrics=metrics,
    ))

    # --- Third station: pressen_3 ---
    p3 = step_cfg(cfg, "pressen_3")
    env.process(run_serial_station(
        env,
        name="pressen_3",
        input_store=buffers["after_pressen_2"],   # consume output of pressen_2
        output_store=buffers["after_pressen_3"],  # TEMP buffer for visibility
        cycle_time_s=p3["cycle_time_s"],          # seconds in YAML; we divide by 60 in the station
        workers_required=p3.get("workers_required", 0),
        metrics=metrics,
    ))

    # Run
    horizon = cfg["meta"]["horizon_min"]
    env.run(until=horizon)

    # KPIs (simple)
    # Utilization % = busy_time / horizon
    util = []
    for st, busy in metrics["station_busy_time"].items():
        util.append({"Station": st, "Utilization %": round(100 * busy / horizon, 2)})
    kpi_stations = pd.DataFrame(util) if util else pd.DataFrame(columns=["Station","Utilization %"])

    kpi_general = pd.DataFrame(
        [
            {"KPI": "New orders arrived", "Value": metrics["arrivals_new"]},
            {"KPI": "Returns arrived", "Value": metrics["arrivals_returns"]},
            {"KPI": "Neu-Lager level (end)", "Value": len(buffers["neu_lager"].items)},
            {"KPI": "after_pressen_1 level (end)", "Value": len(buffers["after_pressen_1"].items)},
            {"KPI": "pressen_1 output", "Value": metrics["station_output"].get("pressen_1", 0)},
            {"KPI": "after_pressen_2 level (end)", "Value": len(buffers["after_pressen_2"].items)},
            {"KPI": "pressen_2 output", "Value": metrics["station_output"].get("pressen_2", 0)},
            {"KPI": "after_pressen_3 level (end)", "Value": len(buffers["after_pressen_3"].items)},
            {"KPI": "pressen_2 output", "Value": metrics["station_output"].get("pressen_3", 0)},
            {"KPI": "Lost due to Neu-Lager full", "Value": metrics["lost_new_due_to_neu_lager_full"]},
        ]
    )

    inventory_ts = pd.DataFrame(columns=["time_min", *buffers.keys()])

    return {
        "kpi_general": kpi_general,
        "kpi_stations": kpi_stations,
        "inventory_ts": inventory_ts,
        "buffers": list(buffers.keys()),
        "log": "pressen_1 running (pulls from neu_lager) + arrivals (new & returns).",
    }
