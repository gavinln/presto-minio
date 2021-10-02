## Ibis

### Extract the Postgres sample database

1. Change to the sample database directory

```
cd postgres
```

2. Extract the sample database

```
tar xvfz dvdrental.tar.gz
```

3. Connect to Docker container

```
CONTAINER_ID=$(docker container ls -f "ancestor=postgres:12.4" -q)
docker exec -ti $CONTAINER_ID bash
```

4. Exit from Docker container

```
exit
```

5. Create databse dvdrental

```
SQL="create database dvdrental"
docker exec -ti $CONTAINER_ID psql -U postgres -d postgres -c "$SQL"
```

6. Load database

```
docker exec -ti $CONTAINER_ID pg_restore -U postgres -d dvdrental /dvdrental.tar
```

[Ibis][500] provides a standard way to write analytics code, that then can be run in
multiple engines both SQL and non-SQL systems.

[500]: http://ibis-project.org/

The GPU database [Omnisci][510] has one of the best documentation of IBIS of any
backend.

[510]: https://docs-new.omnisci.com/data-science/introduction-to-ibis

This [article][520] demonstrates how to use ibis with Bigquery.

[520]: https://towardsdatascience.com/ibis-a-framework-to-tie-together-development-and-production-code-588d05e07d11

Ibis Spark backend [video][530]

[530]: https://www.youtube.com/watch?v=vCipYUrlNQI

Writing an [Ibis backend][540]

[540]: https://github.com/ibis-project/ibis/issues/2307 

[Notebook][550] with Ibis tutorial

[550]: https://cloud.google.com/community/tutorials/bigquery-ibis

[Ibis and Altair][560] for data exploration

[560]: https://www.youtube.com/watch?v=1AUaddf5tk8

Code sizes for backends

```
$ ls ibis/sql/postgres/*.py | xargs -I {} cat {} | wc
   1097    3173   33393

$ ls ibis/sql/mysql/*.py | xargs -I {} cat {} | wc
    626    1524   17822

$ ls ibis/sql/sqlite/*.py | xargs -I {} cat {} | wc
    800    1930   20562

$ ls ibis/omniscidb/*.py | xargs -I {} cat {} | wc
   3931    9214  105421

$ ls ibis/bigquery/*.py | xargs -I {} cat {} | wc
   1385    3221   39921

$ ls ibis/clickhouse/*.py | xargs -I {} cat {} | wc
   1530    3484   42067

$ ls ibis/impala/*.py | xargs -I {} cat {} | wc
   5627   13758  158256

$ ls ibis/pyspark/*.py | xargs -I {} cat {} | wc
   1605    3969   43782

$ ls ibis/spark/*.py | xargs -I {} cat {} | wc
   1760    4080   49838
```
