SELECT create_distributed_table ('hub_agreement', 'primary_key');
SELECT create_distributed_table ('sat_agreement', 'primary_key', colocate_with => 'hub_agreement');
SELECT create_reference_table('ref_products');