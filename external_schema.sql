create schema dbs_external_another.landing
managed location 's3://spark-read-study-bucket/catalogs/dbs_external_another/landing';

describe schema dbs_external_another.landing;