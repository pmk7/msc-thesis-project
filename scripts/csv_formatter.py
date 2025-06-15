import os
import pandas as pd
import argparse


# Script location: /scripts
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")


# Metrics to evaluate (duration is time, not energy)
ENERGY_METRICS = ['cpu_energy', 'ram_energy', 'energy_consumed']
ALL_METRICS = ENERGY_METRICS + ['duration']

CRUD_ORDER = [
    "create_customer",
    "get_customers",
    "get_customer_by_id",
    "fetch_top_spending_customers",
    "update_customer_email",
    "update_many_contract_types",
    "delete_inactive_customers",
    "delete_customer_by_id"
]


def parse_filename(filename):
    parts = filename.replace('.csv', '').split('_')
    query_name = '_'.join(parts[1:-1])
    record_size = parts[-1]
    return query_name, int(record_size)


def load_metric_values(filepath, metrics):
    df = pd.read_csv(filepath)
    return {metric: df[metric].dropna().astype(float).tolist() for metric in metrics}


def format_result(orm_avg, sql_avg):
    if orm_avg == 0 or sql_avg == 0:
        return "n/a"
    diff = ((orm_avg - sql_avg) / sql_avg) * 100
    label = "orm" if diff > 0 else "sql"
    return f"{label} {abs(round(diff))}%"


def process_record_size(record_count):
    orm_dir = os.path.join(RESULTS_DIR, f"{record_count}", f"orm_{record_count}_v2")
    sql_dir = os.path.join(RESULTS_DIR, f"{record_count}", f"sql_{record_count}_v2")
    summary_dir = os.path.join(RESULTS_DIR, f"{record_count}", "comparison")
    summary_file = os.path.join(summary_dir, f"{record_count}_energy_comparison_summary.csv")

    if not os.path.exists(orm_dir) or not os.path.exists(sql_dir):
        print(f"Skipping {record_count}: missing folders.")
        return

    os.makedirs(summary_dir, exist_ok=True)

    orm_files = [f for f in os.listdir(orm_dir) if f.endswith('.csv') and not f.startswith("baseline")]
    comparison_rows = []

    for orm_file in orm_files:
        query_name, _ = parse_filename(orm_file)
        sql_file = f"sql_{query_name}_{record_count}.csv"

        orm_path = os.path.join(orm_dir, orm_file)
        sql_path = os.path.join(sql_dir, sql_file)

        if not os.path.exists(sql_path):
            print(f"Skipping {query_name}_{record_count}: SQL file not found.")
            continue

        orm_data = load_metric_values(orm_path, ALL_METRICS)
        sql_data = load_metric_values(sql_path, ALL_METRICS)

        row = {
            "query": query_name,
            "record_size": record_count
        }

        for metric in ALL_METRICS:
            orm_list = orm_data.get(metric, [])
            sql_list = sql_data.get(metric, [])

            orm_avg = sum(orm_list) / len(orm_list) if orm_list else 0.0
            sql_avg = sum(sql_list) / len(sql_list) if sql_list else 0.0

            row[f"orm_{metric}_values"] = ";".join([f"{x:.6g}" for x in orm_list])
            row[f"sql_{metric}_values"] = ";".join([f"{x:.6g}" for x in sql_list])
            row[f"orm_{metric}_avg"] = round(orm_avg, 10)
            row[f"sql_{metric}_avg"] = round(sql_avg, 10)

            if metric in ENERGY_METRICS:
                row[f"orm_{metric}_avg_joules"] = round(orm_avg * 3_600_000, 6)
                row[f"sql_{metric}_avg_joules"] = round(sql_avg * 3_600_000, 6)

            row[f"result_{metric}"] = format_result(orm_avg, sql_avg)

        comparison_rows.append(row)

    df = pd.DataFrame(comparison_rows)

    df["query"] = pd.Categorical(df["query"], categories=CRUD_ORDER, ordered=True)
    df.sort_values(by="query", inplace=True)
    df.to_csv(summary_file, index=False)
    print(f"Saved summary for {record_count} records to:\n{summary_file}")


def create_cross_record_comparison():
    """Create a comparison file showing energy consumption across different record sizes."""
    summaries = []
    record_sizes = []

    for folder in sorted(os.listdir(RESULTS_DIR), key=lambda x: int(x) if x.isdigit() else float('inf')):
        if folder.isdigit():
            record_size = int(folder)
            summary_path = os.path.join(RESULTS_DIR, folder, "comparison", f"{folder}_energy_comparison_summary.csv")
            if os.path.exists(summary_path):
                df = pd.read_csv(summary_path)
                record_sizes.append(record_size)
                summaries.append(df)

    if not summaries:
        print("No summary files found for cross-record comparison.")
        return

    # Initialize result data
    result_data = []

    # Process each query according to CRUD_ORDER
    for query in CRUD_ORDER:
        row = {'query': query}

        for i, record_size in enumerate(record_sizes):
            summary_df = summaries[i]
            query_data = summary_df[summary_df['query'] == query]

            if not query_data.empty:
                orm_energy = query_data.get('orm_energy_consumed_avg_joules', pd.Series([0])).iloc[0]
                sql_energy = query_data.get('sql_energy_consumed_avg_joules', pd.Series([0])).iloc[0]

                row[f'{record_size}_orm_joules'] = orm_energy
                row[f'{record_size}_sql_joules'] = sql_energy

                if orm_energy and sql_energy:
                    # percentage diff
                    diff_pct = ((orm_energy - sql_energy) / sql_energy) * 100
                    # determine which is larger and compute multiplier
                    if diff_pct > 0:
                        label = "orm"
                        multiplier = orm_energy / sql_energy
                    else:
                        label = "sql"
                        multiplier = sql_energy / orm_energy

                    # combine into one string: e.g. "orm 2.3x (130%)"
                    row[f'{record_size}_diff'] = (
                        f"{label} {multiplier:.1f}x "
                        f"({abs(diff_pct):.0f}%)"
                    )
                else:
                    row[f'{record_size}_diff'] = "n/a"

        result_data.append(row)

    cross_record_df = pd.DataFrame(result_data)
    output_path = os.path.join(RESULTS_DIR, "cross_record_energy_comparison.csv")
    cross_record_df.to_csv(output_path, index=False)
    print(f"Cross-record comparison saved to:\n{output_path}")


def create_cross_record_comparison_trimmed():
    """Create a comparison file excluding the highest+lowest energy_consumed runs."""
    summaries = []
    record_sizes = []

    # load each record-size summary
    for folder in sorted(os.listdir(RESULTS_DIR), key=lambda x: int(x) if x.isdigit() else float('inf')):
        if not folder.isdigit():
            continue
        record_size = int(folder)
        record_sizes.append(record_size)

        summary_path = os.path.join(
            RESULTS_DIR, folder, "comparison", f"{folder}_energy_comparison_summary.csv"
        )
        if os.path.exists(summary_path):
            summaries.append(pd.read_csv(summary_path))

    if not summaries:
        print("No summary files found for trimmed cross-record comparison.")
        return

    result_data = []

    for query in CRUD_ORDER:
        row = {"query": query}

        for i, record_size in enumerate(record_sizes):
            df = summaries[i]
            qdf = df[df["query"] == query]
            if qdf.empty:
                row[f"{record_size}_orm_trim_joules"] = ""
                row[f"{record_size}_sql_trim_joules"] = ""
                row[f"{record_size}_trim_diff"] = "n/a"
                continue

            # parse the raw energy_consumed runs
            orm_raw = qdf["orm_energy_consumed_values"].iloc[0]
            orm_vals = []
            if isinstance(orm_raw, str):
                orm_vals = [float(x) for x in orm_raw.split(";") if x]
            elif isinstance(orm_raw, (float, int)):
                orm_vals = [float(orm_raw)]

            sql_raw = qdf["sql_energy_consumed_values"].iloc[0]
            sql_vals = []
            if isinstance(sql_raw, str):
                sql_vals = [float(x) for x in sql_raw.split(";") if x]
            elif isinstance(sql_raw, (float, int)):
                sql_vals = [float(sql_raw)]

            def trimmed_average(vals):
                if len(vals) > 2:
                    s = sorted(vals)[1:-1]
                else:
                    s = vals
                return sum(s) / len(s) if s else 0.0

            orm_avg_trim = trimmed_average(orm_vals)
            sql_avg_trim = trimmed_average(sql_vals)

            orm_joules = orm_avg_trim * 3_600_000
            sql_joules = sql_avg_trim * 3_600_000

            row[f"{record_size}_orm_trim_joules"] = round(orm_joules, 6)
            row[f"{record_size}_sql_trim_joules"] = round(sql_joules, 6)

            if orm_joules and sql_joules:
                diff_pct = ((orm_joules - sql_joules) / sql_joules) * 100
                if diff_pct > 0:
                    label, mult = "orm", orm_joules / sql_joules
                else:
                    label, mult = "sql", sql_joules / orm_joules
                row[f"{record_size}_trim_diff"] = f"{label} {mult:.1f}x ({abs(diff_pct):.0f}%)"
            else:
                row[f"{record_size}_trim_diff"] = "n/a"

        result_data.append(row)

    trimmed_df = pd.DataFrame(result_data)
    out_path = os.path.join(RESULTS_DIR, "cross_record_energy_comparison_trimmed.csv")
    trimmed_df.to_csv(out_path, index=False)
    print(f"Trimmed cross-record comparison saved to:\n{out_path}")


def main():
    p = argparse.ArgumentParser(
        description="Generate per-size summaries and cross-record comparisons."
    )
    p.add_argument(
        "-r", "--records",
        help="Comma-separated record sizes to process (e.g. 100,500,1000). "
             "If omitted, all digit-named folders under results/ are used.",
        default=None
    )
    args = p.parse_args()

    if args.records:
        # parse & dedupe, ignore non-ints
        sizes = sorted({int(r) for r in args.records.split(",") if r.isdigit()})
    else:
        # pick up every numeric folder under results/
        sizes = sorted(int(d) for d in os.listdir(RESULTS_DIR) if d.isdigit())

    for sz in sizes:
        print(f"\n=== Processing {sz} records ===")
        process_record_size(sz)

    print("\n=== Building full cross-record comparison ===")
    create_cross_record_comparison()

    print("\n=== Building trimmed cross-record comparison ===")
    create_cross_record_comparison_trimmed()


if __name__ == "__main__":
    main()