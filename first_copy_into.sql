use catalog dbs_study;
SHOW VOLUMES;

create or replace table dbs_study.landing.customers_v1 (

    id int,
    name string,
    age int
);

-- con of copy into
-- if two file is uploading, file a is uploaded and b is uplaoding, trigger copy into
-- result only a is uploaded instead of b. that's why auto_loader can solve indepodent
copy into dbs_study.landing.customers_v1
--by defualt withou grand permission you can't copy the other catalog volume data
from '/Volumes/dbs_study/landing/customers_vol/customers_info'
FILEFORMAT = csv
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');

select * from dbs_study.landing.customers_v1;