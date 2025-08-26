# apps/run_sim.py
from pathlib import Path
from learning_factory.config import load_config
from learning_factory.simulate import run_simulation

def _save_csv_bundle(res):
    outdir = Path(res.get("outdir", "runs/_latest"))
    outdir.mkdir(parents=True, exist_ok=True)

    def save(df, name):
        if name in res and hasattr(res[name], "to_csv"):
            res[name].to_csv(outdir / f"{name}.csv", index=False)

    # persist the key tables
    for name in [
        "kpi_general", "kpi_stations", "kpi_wip_now", "kpi_wip_peak",
        "kpi_downtime", "resource_kpis", "labor_kpis", "cost_kpis",
        "inventory_ts"
    ]:
        save(res, name)

    # also drop a quick README.txt
    (outdir / "README.txt").write_text(res.get("log", ""), encoding="utf-8")
    print(f"\nSaved run outputs to: {outdir.as_posix()}")

def main():
    cfg = load_config("configs/stellmotor_baseline.yaml")
    res = run_simulation(cfg)

    print("\n--- General KPIs ---")
    print(res["kpi_general"].to_string(index=False))

    if not res["kpi_wip_now"].empty:
        print("\n--- In-Station WIP (live, end) ---")
        print(res["kpi_wip_now"].to_string(index=False))
    if not res["kpi_wip_peak"].empty:
        print("\n--- Peak In-Process by Station ---")
        print(res["kpi_wip_peak"].to_string(index=False))
    if not res["kpi_downtime"].empty:
        print("\n--- Downtime (min) ---")
        print(res["kpi_downtime"].to_string(index=False))

    if not res["kpi_stations"].empty:
        print("\n--- Station Utilization ---")
        print(res["kpi_stations"].to_string(index=False))

    if "resource_kpis" in res and not res["resource_kpis"].empty:
        print("\n--- Resource KPIs ---")
        print(res["resource_kpis"].to_string(index=False))
    if "labor_kpis" in res and not res["labor_kpis"].empty:
        print("\n--- Labor KPIs ---")
        print(res["labor_kpis"].to_string(index=False))
    if "cost_kpis" in res and not res["cost_kpis"].empty:
        print("\n--- Cost Summary ---")
        print(res["cost_kpis"].to_string(index=False))

    print("\n--- Inventory/WIP Time Series (preview) ---")
    print(res["inventory_ts"].head(3).to_string(index=False))

    print("\n--- Buffers in this model ---")
    print(res["buffers"])

    print("\n--- Log ---")
    print(res["log"])

    # NEW: persist CSVs
    _save_csv_bundle(res)

if __name__ == "__main__":
    main()
