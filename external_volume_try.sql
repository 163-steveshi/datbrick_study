use catalog  dbs_external_another;
describe volume dbs_external_another.landing.customer_test_vol;

create table dbs_external_another.landing.customer_external (
    customer_id INT,
    customer_name STRING,
    age INT
)
-- this for using the existed location path must has the delta keyword
-- USING DELTA location
USING DELTA
location 's3://spark-read-study-bucket/catalogs/dbs_external_another/landing/customer_test_vol/';

describe table extended dbs_external_another.landing.customer_external;