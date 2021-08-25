\timing on
SELECT COUNT(hub_agreement.primary_key), sat_agreement.product_code
FROM hub_agreement
INNER JOIN sat_agreement ON hub_agreement.primary_key = sat_agreement.primary_key
GROUP BY sat_agreement.product_code;