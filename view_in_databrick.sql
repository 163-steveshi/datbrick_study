
drop table if exists dbs_external_another.landing.customer;
create table dbs_external_another.landing.customer
(
    id int,
    name string
);

insert into dbs_external_another.landing.customer values (1, 'John'), (2, 'Jane');
select * from dbs_external_another.landing.customer;

create view dbs_external_another.landing.customer_view as select * from dbs_external_another.landing.customer; 

select * from  dbs_external_another.landing.customer_view;