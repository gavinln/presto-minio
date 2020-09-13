-- botrignt Tnew

-- ssh gavinsvr
-- cd ~/ws/presto-minio/presto-minio
-- java -jar presto-cli.jar

show catalogs;
show schemas from minio;
use minio.default;
show tables;

-- drop table ft_million_rows;
-- drop table million_rows;

-- Create two tables of data
-- First table: 1,000,000 rows
-- 500 grp_code values
-- 2,000 times

-- million row data set: 500 codes x 2k (alt: 500 * 10k = 5m)
    create table minio.default.million_rows as
    with cycle as (
        select sequence(1, 2000) as items
    ),
    data as (
        select
            row_number() over() as id, 
            s.item as grp_code
        from cycle 
            cross join unnest(items) as t(item)
            cross join unnest(items) as s(item)
        where s.item <= 500
    )
    select *
    from data
    ;

-- Second table: 50,000,000
-- 5,000 grp_code values
-- 10,000 times

-- fifty million row data set: 5,000 codes x 10k (alt: 9000 * 10k = 90m)
    create table minio.default.ft_million_rows as
    with cycle as (
        select sequence(1, 10000) as items
    ),
    data as (
        select
            row_number() over() as id, 
            s.item as grp_code
        from cycle 
            cross join unnest(items) as t(item)
            cross join unnest(items) as s(item)
        where s.item >= 101 and s.item <= 5100
    )
    select id + 10000 as id, grp_code
    from data
    ;

-- display stats on each table
    select
        count(distinct(grp_code)) unique_grp_code,
        min(grp_code), max(grp_code), count(grp_code)/1e6 as count_grp_code
    from ft_million_rows
    union all
    select
        count(distinct(grp_code)),
        min(grp_code), max(grp_code), count(grp_code)/1e6 as count_grp_code
    from million_rows
    ;

-- join fifty million rows and million rows: matching on unique values: 5 seconds
    with ftm_unique as (
        select fr.grp_code, count(*) as code_count
        from ft_million_rows fr
        group by fr.grp_code
    ),
    million_unique as (
        select mr.grp_code, count(*) as code_count
        from million_rows mr
        group by mr.grp_code
    ),
    all_join as (
        select biu.grp_code, biu.code_count as bil_code_count, miu.code_count as mil_code_count
        from ftm_unique biu
            full join million_unique miu on
                biu.grp_code = miu.grp_code
    )
    select count(*), 'acount'
    from all_join
    where mil_code_count is NULL
    union all
    select count(*), 'bcount'
    from all_join
    where bil_code_count is NULL
    union all
    select count(*), 'ncount'
    from all_join
    where mil_code_count is not null and bil_code_count is not null
    ;

-- join fifty million rows and million rows using distinct in 1 pass:
    with union_all as (
        select 'ftm' as tbl, grp_code
        from ft_million_rows
        union
        select 'mil', grp_code
        from million_rows
    )
    select tbl, count(distinct grp_code)
    from union_all
    group by rollup (tbl)
    ;

-- join fifty million rows and million rows using approx distinct: 3 seconds
    with ftm_unique as (
        select approx_distinct(fr.grp_code) code_count
        from ft_million_rows fr
    ),
    million_unique as (
        select approx_distinct(mr.grp_code) code_count
        from million_rows mr
    ),
    all_unique as (
        select approx_distinct(grp_code) code_count
        from (
            select grp_code from ft_million_rows
            union
            select grp_code from million_rows
        )
    )
    select 'bil', *
    from ftm_unique
    union
    select 'mil', *
    from million_unique
    union
    select 'all', *
    from all_unique
    ;

-- join fifty million rows and million rows using approx distinct in 1 pass:
    with union_all as (
        select cast(0 as tinyint) as tbl, grp_code
        from ft_million_rows
        union
        select cast(1 as tinyint), grp_code
        from million_rows
    )
    select tbl, approx_distinct(grp_code)
    from union_all
    group by rollup (tbl)
    ;

-- join fifty million rows and million rows using approx set: 2 seconds
    with ftm_unique as (
        select cast(approx_set(grp_code) as varbinary) as code_hll
        from ft_million_rows fr
    ),
    million_unique as (
        select cast(approx_set(grp_code) as varbinary) as code_hll
        from million_rows mr
    ),
    union_all as (
        select 'bil' as tbl, code_hll
        from ftm_unique
        union all
        select 'mil', code_hll
        from million_unique
    )
    select
        tbl, cardinality(merge(cast(code_hll as hyperloglog))) as union_count
    from union_all
    group by rollup (tbl)
    ;

-- 
    select grp_code, count(*)  -- 2 seconds
    from ft_million_rows
    group by 1
    ;

/*
 * Tables stored on external parquet files
 */

-- External table with a million rows using int64
    CREATE TABLE minio.default.million_external
    with (format='parquet', external_location='s3a://example-data/million_external/') as
    select * from minio.default.million_rows
    ;

-- External table with fifty million rows using int64
    CREATE TABLE default.ft_million_external
    with (format='parquet', external_location='s3a://example-data/ft_million_external/') as
    select * from ft_million_rows
    ;

-- External table with fifty million rows using int32
    CREATE TABLE default.ft_million_external_typed
    with (format='parquet', external_location='s3a://example-data/ft_million_external_typed/') as
    select cast(id as int) id, cast(grp_code as int) grp_code from ft_million_rows
    ;

-- Fails with out of range for external table with million rows using int8
    CREATE TABLE default.million_external_typed_tiny
    with (format='parquet', external_location='s3a://example-data/million_external_typed_tiny/') as
    select cast(id as tinyint) id, cast(grp_code as tinyint) grp_code from million_rows
    ;

-- 
    select grp_code, count(*)  -- 6 seconds
    from ft_million_external
    group by 1
    ;


/*
 * Optimize data by bucketing (clustered)
 */

-- 
    CREATE TABLE default.external_clustered
    with (
        format = 'parquet',
        external_location = 's3a://example-data/external-clustered/',
        bucketed_by = ARRAY['grp_code'],
        bucket_count = 100
    ) as
    select * from ft_million_rows
    ;

    select grp_code, count(*)  -- 4 seconds
    from external_clustered
    group by 1
    ;

-- Count for external table
    select count(*)  -- 4 seconds
    from (
        select grp_code, avg(id) from ft_million_external where grp_code < 200 group by grp_code
    );

-- Count for clustered table
    select count(*)  -- 2.5 seconds
    from (
        select grp_code, avg(id) from external_clustered where grp_code < 200 group by grp_code
    );

-- Approx distinct count
    select count(*)
    from (
        select grp_code, approx_distinct(id)
        from external_clustered
        group by 1
    );

-- fails with query exceeded per-node user memory limit of 104.0MB
    select grp_code, count(distinct id)
    from external_clustered
    group by 1
    ;

-- drop table external_clustered; 

/*
 * Different kinds table
 * Hive internal
 * External table on minio version of S3
 * Bucketed table
 */

-- external table join timing: 1:57
    with join_all as (
        select ftm.id as f_id, mr.id as m_id
        from ft_million_external ftm
            join million_rows mr on
                ftm.grp_code = mr.grp_code
        where
            ftm.grp_code < 201
    )
    select count(*)/1e6
    from join_all
    ;

-- bucketed table join timing: 0:44
    with join_all as (
        select ftm.id as f_id, mr.id as m_id
        from external_clustered ftm
            join million_rows mr on
                ftm.grp_code = mr.grp_code
        where
            ftm.grp_code < 201
    )
    select count(*)/1e6
    from join_all
    ;

/*
 * Join table with aggregated results
 */

-- join table counts with union timings: 3.85
    with a_table as (
        select grp_code, sum(1) as code_count
        from ft_million_rows
        group by grp_code
    ),
    b_table as (
        select grp_code, sum(1) as code_count
        from million_rows
        group by grp_code
    ),
    union_all as (
        select grp_code, code_count as a_count, 0 as b_count
        from a_table
        union all
        select grp_code, 0 as a_count, code_count as b_count
        from b_table
    ),
    grp_counts as (
        select grp_code, sum(a_count) as a_count, sum(b_count) as b_count
        from union_all
        group by grp_code
    )
    select count(*), sum(a_count), sum(b_count)
    from grp_counts
    ;

-- join table counts with outer join timings: 3.66
    with a_table as (
        select grp_code, sum(1) as a_count
        from ft_million_rows
        group by grp_code
    ),
    b_table as (
        select grp_code, sum(1) as b_count
        from million_rows
        group by grp_code
    ),
    join_all as (
        select grp_code, a_count, b_count
        from a_table
            full join b_table using(grp_code)
    )
    select count(*), sum(a_count), sum(b_count)
    from join_all
    ;

/*
 * Distinct counts
 */

-- join fifty million rows and million rows using approx set: 1.92
    with ftm_unique as (
        select cast(approx_set(grp_code) as varbinary) as code_hll, count(*) as counts
        from ft_million_rows fr
    ),
    million_unique as (
        select cast(approx_set(grp_code) as varbinary) as code_hll, count(*) as counts
        from million_rows mr
    ),
    union_all as (
        select 'ftm' as tbl, code_hll, counts
        from ftm_unique
        union all
        select 'mil', code_hll, counts
        from million_unique
    )
    select
        tbl,
        cardinality(merge(cast(code_hll as hyperloglog))) as union_count,
        sum(counts)
    from union_all
    group by rollup (tbl)
    ;

-- join fifty million rows and million rows using approx set: 10.22
    with ftm_unique as (
        select count(distinct grp_code) as count_distinct, count(*) as counts
        from ft_million_rows fr
    ),
    million_unique as (
        select count(distinct grp_code) as count_distinct, count(*) as counts
        from million_rows mr
    ),
    union_all as (
        select distinct grp_code as grp_code
        from ft_million_rows
        union
        select distinct grp_code
        from million_rows
    )
    select count(grp_code), NULL
    from union_all
    union all
    select count_distinct, counts
    from ftm_unique
    union all
    select count_distinct, counts
    from million_unique
    ;

/*
 * Set operations: union, intersection
 */
-- approx set operations: 1.96 seconds
    with union_all as (
        select
            'ftm' as tbl,
            cast(approx_set(id) as varbinary) as code_hll,
            count(*) as counts
        from ft_million_rows fmr
        union all
        select
            'mil',
            cast(approx_set(id) as varbinary) as code_hll,
            count(*) as counts
        from million_rows mir
    )
    select
        if (tbl is null, 'union', tbl) set_name,
        cardinality(merge(cast(code_hll as hyperloglog))) as distinct_count
    from union_all
    group by rollup(tbl)
    ;

-- set operations: 11.26s
    select 'ftm', count(id) as counts
    from ft_million_rows
    union all
    select 'mil', count(id) as counts
    from million_rows
    union all
    select 'union', count(*)
    from ft_million_rows fmr
        full join million_rows mir
            on fmr.id = mir.id
    ;

/*
 * partitioned tables
 */
-- partitioned tables
    CREATE TABLE minio.default.million_partitioned
    with (
        format = 'parquet',
        external_location = 's3a://example-data/million-partitioned/',
        partitioned_by = ARRAY['grp_code']
    ) as
    select * from minio.default.million_rows
    with no data
    ;

    insert into minio.default.million_partitioned
    select *
    from minio.default.million_rows
    where (grp_code - 1)/100 = 0
    ;

    insert into minio.default.million_partitioned
    select *
    from minio.default.million_rows
    where (grp_code - 1)/100 = 1
    ;

    insert into minio.default.million_partitioned
    select *
    from minio.default.million_rows
    where (grp_code - 1)/100 = 2
    ;

    insert into minio.default.million_partitioned
    select *
    from minio.default.million_rows
    where (grp_code - 1)/100 = 3
    ;

    insert into minio.default.million_partitioned
    select *
    from minio.default.million_rows
    where (grp_code - 1)/100 = 4
    ;

-- presto different data types
    create table minio.default.multi_types as
    select *
    from (
        values
            (true, cast(1 as tinyint), cast(200 as smallint), cast(300000 as integer),
             cast(4000000000 as bigint), cast(5 as real), cast(6 as double), 'abc')
    ) as x(boolean_t, tinyint_t, smallint_t, integer_t, bigint_t, real_t, double_t, varchar_t)
    ;
