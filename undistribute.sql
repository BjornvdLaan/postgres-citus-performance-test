/* apologies for the dirty hack */

ALTER TABLE sat_agreement DROP CONSTRAINT fk_agreement;
SELECT undistribute_table('hub_agreement');
SELECT undistribute_table('sat_agreement');
ALTER TABLE sat_agreement ADD CONSTRAINT fk_agreement FOREIGN KEY (primary_key) REFERENCES hub_agreement;