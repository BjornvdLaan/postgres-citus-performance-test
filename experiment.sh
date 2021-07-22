#!/bin/bash
HOSTNAME=$1
DBNAME=$2
USER=$3

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
    cat csv/reference_$1.csv | psql_call_with_timing "\COPY ref_products(product_code, product_name, product_category, load_dts, rec_src) FROM STDIN WITH(FORMAT CSV, DELIMITER ',', HEADER FALSE);" -c "TRUNCATE ref_products CASCADE;" "$2-insert-ref-$1.txt"
}

psql_execute_experiment() {
    psql_truncate

    echo "$1 Hub insertions"
    psql_hub_insert $1 $3
    echo "$(( $1*$2 )) Sat insertions"
    psql_sat_insert $(( $1*$2 )) $3
    echo "$(( $1*$2 )) Ref insertions"
    psql_reference_insert $(( $1*$2 )) $3

    echo "Querying all $1 Hub entries, $(( $1*$2 )) Sat entries"
    psql_call_sql_script_print_time_to_file select_all.sql "$3-select-hub-$1-sat-$(( $1*$2 ))-ref-0.txt"

    echo "Querying all $1 Hub entries, $(( $1*$2 )) Sat entries and $(( $1*$2 )) Reference entries"
    psql_call_sql_script_print_time_to_file select_all_with_ref.sql "$3-select-hub-$1-sat-$(( $1*$2 ))-ref-$(( $1*$2 )).txt"

    psql_truncate
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

# Script starts here

echo "Experiment initiated.."
psql_call_sql_script set_up_tables.sql

NUM_OF_RUNS=1
NUM_OF_HUBS=10000

# Perform multiple runs to later calculate an average
for RUN_ID in $(seq 1 $NUM_OF_RUNS)
do
    # Multipliers: 10, 100, 500, 2500, 10000, 50000
    for NUM_OF_SATS_AND_REFS in 10 
    do
        echo "Executing experiment in base scenario.."
        psql_execute_experiment $NUM_OF_HUBS $NUM_OF_SATS_AND_REFS "$RUN_ID-base-scenario"

        echo "Executing experiment with distributed tables.."
        psql_distribute_on_pk
        psql_execute_experiment $NUM_OF_HUBS $NUM_OF_SATS_AND_REFS "$RUN_ID-distribute-pk-scenario"
        psql_undistribute
    done
done

echo "Experiment completed!"