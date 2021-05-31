-- botrignt Tnew

-- ssh gavinsvr
-- cd ~/ws/presto-minio/presto-minio
-- docker exec -ti $(docker container ls -f "ancestor=prestodb/cdh5.15-hive" -q) bash
-- # run one of the two following lines
-- hdfs dfs -ls hdfs://  # to list files
-- /usr/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true

-- https://understandingbigdata.com/category/hive-tutorial/
-- https://cwiki.apache.org/confluence/display/Hive/Home
-- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL

-- 
select version();
show tables;
show compactions;
show transactions;
show functions;
show databases;

-- show commands 1 or 2 parameters
show tables like 'customer*';
show tables in default;
show tables in default like 'customer*';
show table extended in default like 'customer*';
show create table default.example;
show columns in default.example;
show tblproperties default.example;

-- create table in Hive
    create external table example (
        id int,
        name string
    )
    stored as sequencefile;
    ;

    -- drop table example;
    select * from example;

    insert into example
    values
        (1, 'a'),
        (2, 'a'),
        (3, 'a'),
        (1, 'b'),
        (2, 'b'),
        (1, 'c')
    ;

    analyze table example compute statistics;

-- create partitioned table in Hive
    create table partition_example (
        name string
    )
    partitioned by (id int)
    ;

    set hive.exec.dynamic.partition.mode=nonstrict;
    -- set hive.exec.dynamic.partition.mode=strict;

    insert into partition_example partition(id)
    select name, id from example
    ;


    -- drop table partition_example;
    select * from partition_example;

    show partitions partition_example;

    show tables;

-- [CLUSTERED BY (col_name, col_name, ...) [SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS]

describe example;

describe formatted example;

describe extended example;

describe formatted million_partitioned partition (grp_code=200);

set;  -- get all configuration properties

-- get table counts
    select name,
        sum(1) as id_count
    from example
    group by 1
    ;

-- compute table statistics
    analyze table million_rows
    compute statistics
    ;

-- create hive table with parquet compression -- DOES NOT work
    create table parq_compression_snappy (
        id bigint,
        grp_code bigint
    )
    stored as parquet
    location 's3a://example-data/million_external/compression_snappy'
    tblproperties ('parquet.compression'='SNAPPY')
    ;

    insert into parq_compression_snappy
    select * from million_rows
    ;

-- create hive table with parquet compression -- DOES NOT work
    create table parq_compression_gzip (
        id bigint,
        grp_code bigint
    )
    stored as parquet
    location 's3a://example-data/million_external/compression_gzip'
    tblproperties ('parquet.compression'='gzip')
    ;

    insert into parq_compression_gzip
    select * from ft_million_rows
    ;
