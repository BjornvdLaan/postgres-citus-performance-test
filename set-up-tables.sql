DROP TABLE IF EXISTS hub_agreement CASCADE;
DROP TABLE IF EXISTS sat_agreement;
DROP TABLE IF EXISTS ref_products;

CREATE TABLE hub_agreement (
    primary_key varchar(255) PRIMARY KEY,
    unq_ar_id varchar(255) NOT NULL,
    rec_src varchar(255) NOT NULL,
    load_dts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sat_agreement (
    primary_key varchar(255),
    load_dts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    rec_src varchar(255) NOT NULL,
    hash_diff varchar(255) NOT NULL,
    principal int NOT NULL,
    interest int NOT NULL,
    maturity_date timestamp NOT NULL,
    product_code varchar(255) NOT NULL,
    PRIMARY KEY(primary_key, load_dts)
);

CREATE TABLE ref_products (
    product_code varchar(255) PRIMARY KEY,
    product_name varchar(255) NOT NULL,
    product_category varchar(255) NOT NULL,
    load_dts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    rec_src varchar(255) NOT NULL
);

ALTER TABLE sat_agreement ADD CONSTRAINT fk_agreement FOREIGN KEY (primary_key) REFERENCES hub_agreement;