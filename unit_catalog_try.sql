show catalogs;

describe catalog samples; -- describe the meta data
-- describe catalog extended dbs_study; -- describe the meta data + extra info: permission, lcoation, etc
-- drop catalog dbs_study; --usually break beceaue catalog is not empty
-- drop catalog dbs_study cascade; --prefer way cautious

-- drop catalog `dbs_external_example_s3` cascade;

-- command to create external location catalog not via ui
create catalog dbs_external_another
managed location 's3://spark-read-study-bucket/catalogs';
describe catalog extended dbs_external_another; 