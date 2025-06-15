#!/bin/bash

set -e

# base paths
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$PROJECT_ROOT/data"
SEED_SCRIPT="$PROJECT_ROOT/scripts/seed_database.py"
FORMATTER_SCRIPT="$PROJECT_ROOT/scripts/csv_formatter.py"
ORM_TRACKER="$PROJECT_ROOT/src/orm_experiments/orm_energy_tracker_v2.py"
SQL_TRACKER="$PROJECT_ROOT/src/sql_experiments/sql_energy_tracker_v2.py"
RESTART_SCRIPT="$PROJECT_ROOT/scripts/restart_postgres.py"

# record sizes
RECORD_SIZES=(1000 2000 4000 8000 16000 32000 64000 128000 256000 512000 1024000)

#parameters
REPEAT_COUNT=10
SLEEP_DURATION=5

for TARGET_RECORD_COUNT in "${RECORD_SIZES[@]}"; do
    DATA_FILE="$DATA_DIR/fake_data_${TARGET_RECORD_COUNT}.csv"

    echo ""
    echo "======================================"
    echo "Experimental test for dataset: ${TARGET_RECORD_COUNT} records"
    echo "Running ${REPEAT_COUNT} repetitions"
    echo "======================================"

    # orm run
    for i in $(seq 1 $REPEAT_COUNT); do
        echo ""
        echo "Restarting ORM Postgres instance..."
        python3 "$RESTART_SCRIPT" --orm
        echo "----- ORM Run $i -----"
        echo "Seeding ORM database..."
        USE_ORM=true python3 "$SEED_SCRIPT" --data-path "$DATA_FILE"

        echo "Running ORM tracker..."
        RECORD_COUNT=$TARGET_RECORD_COUNT PYTHONPATH="$PROJECT_ROOT" python3 "$ORM_TRACKER"

        echo "Sleeping ${SLEEP_DURATION}s before next run..."
        sleep $SLEEP_DURATION

        echo "Restarting ORM Postgres instance..."
        python3 "$RESTART_SCRIPT" --orm
    done

    # sql run
    for i in $(seq 1 $REPEAT_COUNT); do
        echo ""
        echo "Restarting ORM Postgres instance..."
        python3 "$RESTART_SCRIPT" --sql
        echo "----- SQL Run $i -----"
        echo "Seeding SQL database..."
        USE_ORM=false python3 "$SEED_SCRIPT" --data-path "$DATA_FILE"

        echo "Running SQL tracker..."
        RECORD_COUNT=$TARGET_RECORD_COUNT PYTHONPATH="$PROJECT_ROOT" python3 "$SQL_TRACKER"

        echo "Sleeping ${SLEEP_DURATION}s before next run..."
        sleep $SLEEP_DURATION

        echo "Restarting SQL Postgres instance..."
        python3 "$RESTART_SCRIPT" --sql
    done

    echo ""
    echo "Running formatter..."
    python3 "$FORMATTER_SCRIPT"

    echo ""
    echo "Experimental test for ${TARGET_RECORD_COUNT} records complete"
done