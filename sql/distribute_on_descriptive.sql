SELECT create_distributed_table ('sat_agreement', 'principal');
SELECT create_distributed_table ('hub_agreement', 'primary_key', colocate_with => 'sat_agreement');
SELECT create_reference_table('ref_products');