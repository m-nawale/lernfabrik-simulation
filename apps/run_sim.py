# apps/run_sim.py
from learning_factory.config import load_config
from learning_factory.simulate import run_simulation

def main():
    # Load configuration from YAML
    cfg = load_config("configs/stellmotor_baseline.yaml")

    # Run the simulation
    res = run_simulation(cfg)

    # Print key performance indicators
    print("\n--- General KPIs ---")
    print(res["kpi_general"].to_string(index=False))

    # Show available buffers (useful check in early stages)
    print("\n--- Buffers in this model ---")
    print(res["buffers"])

    # Log message (status)
    print("\n--- Log ---")
    print(res["log"])

if __name__ == "__main__":
    main()
