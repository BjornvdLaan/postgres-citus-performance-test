#!/bin/bash
HOSTNAME=$1
DBNAME=$2
USER=$3
NUM_OF_RUNS=${4:-1}

psql_call() {
    psql -h $HOSTNAME -d $DBNAME -U $USER -c "$1"
}

psql_call_with_timing() {
    psql -h $HOSTNAME -d $DBNAME -U $USER -c "\timing" -c "$1"
}

psql_call_with_timing_to_file() {
    psql -h $HOSTNAME -d $DBNAME -U $USER -c "\timing" -c "$1" | grep "^Time:" >> ./results/$2
}

psql_call_sql_script() {
    psql -h $HOSTNAME -d $DBNAME -U $USER < sql/$@
}

psql_call_sql_script_print_time() {
    psql -h $HOSTNAME -d $DBNAME -U $USER < sql/$@ | grep "^Time:"
}

psql_call_sql_script_print_time_to_file() {
    psql -h $HOSTNAME -d $DBNAME -U $USER < sql/$1 | grep "^Time:" >> ./results/$2
}

psql_truncate() {
    psql_call "TRUNCATE hub_agreement CASCADE;"
    psql_call "TRUNCATE ref_products;"
}

psql_hub_insert() {
    cat csv/hub_$1.csv | psql_call_with_timing_to_file "\COPY hub_agreement(primary_key, unq_ar_id, rec_src, load_dts) FROM STDIN WITH(FORMAT CSV, DELIMITER ',', HEADER FALSE);" "$2-insert-hub-$1.txt"
}

psql_sat_insert() {
    cat csv/sat_$1.csv | psql_call_with_timing_to_file "\COPY sat_agreement(primary_key, load_dts, rec_src, hash_diff, principal, interest, maturity_date, product_code) FROM STDIN WITH(FORMAT CSV, DELIMITER ',', HEADER FALSE);" "$2-insert-sat-$1.txt"
}

psql_reference_insert() {
    cat csv/reference_$1.csv | psql_call_with_timing_to_file "\COPY ref_products(product_code, product_name, product_category, load_dts, rec_src) FROM STDIN WITH(FORMAT CSV, DELIMITER ',', HEADER FALSE);" "$2-insert-ref-$1.txt"
}

psql_execute_experiment() {
    echo "$1 Hub insertions"
    psql_hub_insert $1 $3
    echo "$(( $1*$2 )) Sat insertions"
    psql_sat_insert $(( $1*$2 )) $3
    echo "$(( $1*$2 )) Ref insertions"
    psql_reference_insert $(( $1*$2 )) $3

    # SELECT ALL DATA
    echo "Querying all $1 Hub entries, $(( $1*$2 )) Sat entries"
    psql_call_sql_script_print_time_to_file select_all.sql "$3-select-hub-$1-sat-$(( $1*$2 ))-ref-0.txt"

    echo "Querying all $1 Hub entries, $(( $1*$2 )) Sat entries and $(( $1*$2 )) Reference entries"
    psql_call_sql_script_print_time_to_file select_all_with_ref.sql "$3-select-hub-$1-sat-$(( $1*$2 ))-ref-$(( $1*$2 )).txt"

    # AGGREGATION: COUNT
    echo "Counting all $1 Hub entries by Product Code found in Sat"
    psql_call_sql_script_print_time_to_file count_by_product_code.sql "$3-select-agg-count-group-by-product-code-hub-$1-sat-$(( $1*$2 ))-ref-0.txt"

    echo "Counting all $1 Hub entries by Product Group found in Ref via Sat"
    psql_call_sql_script_print_time_to_file count_by_product_category_with_ref.sql "$3-select-agg-count-group-by-product-category-hub-$1-sat-$(( $1*$2 ))-ref-$(( $1*$2 )).txt"

    # AGGREGATION: SUM
    echo "Summing principal of all $1 Hub entries by Product Code found in Sat ($(( $1*$2 )) entries)"
    psql_call_sql_script_print_time_to_file sum_principal_by_product_code.sql.sql "$3-select-agg-sum-group-by-product-code-hub-$1-sat-$(( $1*$2 ))-ref-0.txt"

    echo "Summing principal of all $1 Hub entries by Product Group found in Ref ($(( $1*$2 )) entries) via Sat ($(( $1*$2 )) entries)"
    psql_call_sql_script_print_time_to_file sum_principal_by_product_category_with_ref.sql "$3-select-agg-sum-group-by-product-category-hub-$1-sat-$(( $1*$2 ))-ref-$(( $1*$2 )).txt"
}

psql_create_indexes() {
    psql_call_sql_script index_on_pk.sql
}

psql_drop_indexes() {
    psql_call_sql_script drop_indexes.sql
}

psql_undistribute() {
    psql_call_sql_script undistribute.sql
}

psql_distribute_on_pk() {
    psql_call_sql_script distribute_on_pk.sql
}

psql_distribute_on_descriptive() {
    psql_call_sql_script distribute_on_descriptive.sql
}

psql_set_up_tables() {
    psql_call_sql_script set_up_tables.sql
}

# Script starts here

NUM_OF_HUBS=10000

# Perform multiple runs if you need (useful when you let it run during night)
for RUN_ID in $(seq 1 $NUM_OF_RUNS)
do
    echo "Run $RUN_ID"
    # Multipliers: 1, 10, 100, 500, 2500
    for NUM_OF_SATS_AND_REFS in 1 10 100 500 2500
    do
        echo "Run $RUN_ID, multiplier = $NUM_OF_SATS_AND_REFS"

        echo "Setting up tables"
        psql_set_up_tables

        echo "Executing experiment in base scenario.."
        psql_execute_experiment $NUM_OF_HUBS $NUM_OF_SATS_AND_REFS "base-scenario"

        echo "Setting up tables"
        psql_set_up_tables

        echo "Executing experiment with distributed tables.."
        psql_distribute_on_pk
        psql_execute_experiment $NUM_OF_HUBS $NUM_OF_SATS_AND_REFS "distribute-pk-scenario"
    done
done

echo "Experiment completed!"