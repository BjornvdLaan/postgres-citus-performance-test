\timing on
SELECT SUM(sat_agreement.principal), sat_agreement.product_code
FROM hub_agreement
INNER JOIN sat_agreement ON hub_agreement.primary_key = sat_agreement.primary_key
GROUP BY sat_agreement.product_code;