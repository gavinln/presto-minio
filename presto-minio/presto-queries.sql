-- botrignt Tnew

-- ssh gavinsvr
-- cd ~/ws/presto-minio/presto-minio
-- java -jar presto-cli.jar

show catalogs;
show schemas from minio;
use minio.default;
show tables;

-- create table
    create table example as
    select * from (
        values
            (1, 'a'),
            (1, 'a'),
            (1, 'a'),
            (2, 'b'),
            (2, 'b'),
            (3, 'c')
    ) as t (id, name)
   ;

-- drop table example;

-- get table counts
    select name,
        sum(1) as id_count
    from example
    group by 1
    ;

-- get histogram
    with item_counts as (
        select histogram(name) as item_count
        from example
    )
    select t.i, t.n
    from item_counts ic
        cross join unnest(ic.item_count) as t(i, n)
    ;
