-- botrignt Tnew

-- ssh gavinsvr
-- cd ~/ws/presto-minio/presto-minio
-- java -jar presto-cli.jar

/*
# Connect o Presto Docker container
docker exec -ti $(docker container ls -f "ancestor=starburstdata/presto" -q) bash
# edit the JVM config file
vi /usr/lib/presto/etc/jvm.config
# modify the lines
-Xmx6G
# edit the Presto config file
vi /usr/lib/presto/etc/config.properties
# add to the file
query.max-memory=5GB
query.max-memory-per-node=1GB
query.max-total-memory-per-node=2GB
*/

show catalogs;
show schemas from minio;
use minio.default;
show tables;


# Target directory for table 'default.example' already exists: hdfs://hadoop-master:9000/user/hive/warehouse/example

-- create table
    create table example as
    select * from (
        values
            (1, 'a'),
            (2, 'a'),
            (3, 'a'),
            (1, 'b'),
            (2, 'b'),
            (1, 'c')
    ) as t (id, name)
   ;

    show stats for example;

-- display values
    select * from example;

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

-- get approx distinct
    select approx_distinct(name) as approx_name_count
    from example
    ;

-- get exact count
    select count(distinct name) as exact_name_count
    from example
    ;

/* Array functions */

-- access elements of an array and get lengths
    SELECT id_val, id_val[1] + id_val[2] as sum_id_val, cardinality(id_val)
    FROM (
        VALUES
            (ARRAY [1, 2]),
            (ARRAY [1, 3]),
            (ARRAY [1, 4]),
            (ARRAY [2, 20]),
            (ARRAY [2, 30]),
            (ARRAY [2, 40])
    ) AS t(id_val)
    ;

-- unnest an array sequence
    with data as (
        select sequence(1, 3) as items
    )
    select items, item
    from data
        cross join unnest(items) as t(item)
    ;


-- unnest an array repeat
    with data as (
        select repeat(1, 10) as items
    )
    select item, floor(random(3)) as rand_int
    from data
        cross join unnest(items) as t(item)
    ;

-- create a grouped data set
    with cycle as (
        select sequence(1, 3) as items
    ),
    data as (
        select
            row_number() over() as id, 
            s.item as grp_code
        from cycle 
            cross join unnest(items) as t(item)
            cross join unnest(items) as s(item)
    )
    select grp_code, sum(id)
    from data
    group by grp_code
    ;


-- million row data set
    create table minio.default.million_row_data as
    with cycle as (
        select sequence(1, 1000) as items
    ),
    data as (
        select
            row_number() over() as id, 
            s.item as grp_code
        from cycle 
            cross join unnest(items) as t(item)
            cross join unnest(items) as s(item)
    )
    select *
    from data
    ;

-- billion row data set
    create table minio.default.billion_row_data as
    with cycle as (
        select sequence(1, 1000) as items
    ),
    data as (
        select
            row_number() over() as id, 
            s.item as grp_code
        from cycle 
            cross join unnest(items) as t(item)
            cross join unnest(items) as s(item)
            cross join unnest(items) as u(item)
    )
    select *
    from data
    ;

    -- Using Minio browser http://10.0.0.2:9000/ create bucket example-data
    CREATE TABLE default.billion_rows
    with (format='parquet', external_location='s3a://example-data/billion-rows/') as
    select * from billion_row_data
    ;


select count(*) from billion_row_data;

select grp_code, avg(id) from billion_row_data where grp_code < 20 group by grp_code;

/* Aggregate functions */

-- array
    select array_agg(name) from example;

    select max(id) from example;

    select max_by(name, id) from example;

    select min_by(name, id, 2) from example;

-- get sum per group
    SELECT id, reduce_agg(value, 0, (a, b) -> a + b, (a, b) -> a + b)
    FROM (
        VALUES
            (1, 2),
            (1, 3),
            (1, 4),
            (2, 20),
            (2, 30),
            (2, 40)
    ) AS t(id, value)
    group by id
    ;

-- get count, sum per group
    SELECT id, reduce_agg(value, 0, (a, b) -> a + b, (a, b) -> a + b)
    FROM (
        VALUES
            (1, 2),
            (1, 3),
            (1, 4),
            (2, 20),
            (2, 30),
            (2, 40)
    ) AS t(id, value)
    group by id
    ;



