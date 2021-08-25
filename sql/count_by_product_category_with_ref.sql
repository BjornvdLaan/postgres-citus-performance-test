\timing on
SELECT COUNT(hub_agreement.primary_key), ref_products.product_category
FROM hub_agreement
INNER JOIN sat_agreement ON hub_agreement.primary_key = sat_agreement.primary_key
INNER JOIN ref_products ON sat_agreement.product_code = ref_products.product_code
GROUP BY ref_products.product_category;