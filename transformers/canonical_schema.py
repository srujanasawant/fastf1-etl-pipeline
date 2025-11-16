# transformers/canonical_schema.py

# This is the unified schema that ALL F1 data will map into.
canonical_f1_model = {
    "driver_id": str,           # ex: "HAM"
    "driver_name": str,         # ex: "Lewis Hamilton"
    "driver_number": str,       # ex: "44"
    "constructor": str,         # ex: "Mercedes"

    "season": int,              # ex: 2023
    "round": int,               # ex: 1
    "race_name": str,           # ex: "Bahrain Grand Prix"
    "race_date": str,           # ex: "2023-03-05"

    "position": int,            # race finishing position (Ergast)
    "grid": int,                # starting grid position
    "points": float,            # points earned

    "lap_number": int,          # lap index
    "lap_time_ms": float,       # lap time in milliseconds
    "sector1_ms": float,
    "sector2_ms": float,
    "sector3_ms": float,

    "speed_trap_kph": float,    # from telemetry
}
