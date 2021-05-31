-- botrignt Tnew

-- ssh gavinsvr
-- cd ~/ws/presto-minio/presto-minio
-- java -jar presto-cli.jar

-- use minio.default;

-- get data
    select count(*) -- abc
    /* abc */
    from million_rows
    ;

-- explain query
    EXPLAIN (TYPE IO, FORMAT JSON)
    with result as (
        select id, grp_code
        from minio.default.million_rows
    )
    select *
    from result
    ;

-- get detail
    select *
    from minio.default.million_partitioned
    limit 10
    ;

-- describe table
    desc minio.default.million_partitioned
    ;

-- prepare statement checks for syntax errors (no check fo rmissing tables
    PREPARE my_select1 FROM
    select *
    from minio.default.million_partitioned
    where id > 1
    ;

-- fails if missing table
    describe input my_select1
    ;

    describe output my_select1
    ;

    deallocate prepare my_select1
    ;

-- explain validates sql for syntax/semantic errors (missing table)
    explain (type validate)
    select * from minio.default.million_partitioned
    ;

-- get input/output in JSON format
    explain (type io)
    select * from minio.default.million_partitioned
    ;

-- create view allows sql statement comments to be removed
    create view my_view1 as
    select *
    /* asdfsdf */
    from minio.default.million_partitioned -- asdfsd
    ;

    show create view my_view1
    ;

    drop view my_view1
    ;

