-- botrignt Tnew

-- ssh gavinsvr
-- cd ~/ws/presto-minio/presto-minio
-- java -jar presto-cli.jar

show catalogs;
show schemas from minio;
use minio.default;
show tables;

-- drop table ft_million_uuid;
-- drop table million_uuid;

-- Create two tables of data
-- First table: 1,000,000 rows
-- 500 grp_code values
-- 2,000 times


-- try uuid
    with test_data as (
        select
            cast(uuid() as varbinary) as uui1,
            cast(uuid() as varchar) as uui2
    )
    select cast(uui1 as uuid), uui2
    from test_data;

/*
 * Join performance is improved using buckets (not using join column)
 * and sorted data on join column
 */
-- five million row data set: 1000 codes x 5k
    create table fmillion_uuid with (
        sorted_by = ARRAY['id'],
        bucketed_by = ARRAY['grp'],
        bucket_count = 10
    ) as
    with cycle as (
        select sequence(1, 5000) as items
    ),
    data as (
        select
            row_number() over() as id, 
            cast(uuid() as varbinary) as uui
        from cycle 
            cross join unnest(items) as t(item)
            cross join unnest(items) as s(item)
        where s.item <= 1000
    )
    select id, uui, cast(id % 5 as int) as grp
    from data
    ;

-- five million row data set: 1000 codes x 5k - 100k
    create table fmillion_uuid2 with (
        sorted_by = ARRAY['id'],
        bucketed_by = ARRAY['grp'],
        bucket_count = 10
    ) as
    with cycle as (
        select sequence(1, 5000) as items
    ),
    data as (
        select
            row_number() over() + 4900000 as id, 
            cast(uuid() as varbinary) as uui
        from cycle 
            cross join unnest(items) as t(item)
            cross join unnest(items) as s(item)
        where s.item <= 980
    )
    select id, uui, cast(id % 5 as int) as grp
    from data
    ;

    insert into fmillion_uuid2
    with max_id as (
        select max(id) as max_id from fmillion_uuid2
    ),
    data as (
        select row_number() over () + max_id as id, uui
        from fmillion_uuid mui, max_id mid
        limit 100000
    )
    select id, uui, cast(id % 5 as int) as grp
    from data
    ;

-- get uuid join using binary values: 3.4 seconds
    select count(*)
    from fmillion_uuid mid
        join fmillion_uuid2 mid2 on
            mid.uui = mid2.uui
    ;

-- get uuid join using bigint id values: 1.66 seconds
    select count(*)
    from fmillion_uuid mid
        join fmillion_uuid2 mid2 on
            mid.id = mid2.id
    ;

