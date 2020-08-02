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
            (2, 'a'),
            (3, 'a'),
            (1, 'b'),
            (2, 'b'),
            (1, 'c')
    ) as t (id, name)
   ;

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

-- unnest an array
    with data as (
        select sequence(1, 3) as items
    )
    select items, item
    from data
        cross join unnest(items) as t(item)
    ;


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



