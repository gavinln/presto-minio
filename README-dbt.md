## DBT project

### Postgres DBT

Setup dbt for a Postgres database

#### Start Docker

1. Check the status of the docker daemon

```
server docker status
```

2. Start the Docker daemon if not running

```
sudo service docker start
```

#### Start the postgres Docker instance

1. Change to the postgres docker directory

```
cd postgres
```

2. Start Postgres and adminer (the user interface)

```
docker-compose up -d
```

3. Connect to Postgres database using the command line interface psql

```
docker-compose exec db psql -U postgres
```

4. List postgres commands

```
\?
```

4. List databases

```
\l
```

5. Connect to the postgres database

```
\c
```

6. List schemas

```
\dn
```

7. List tables for schema dbt_schema

```
\dt dbt_schema.*
```

8. List views for schema dbt_schema

```
\dv dbt_schema.*
```

### Connect to database using adminer in the browser

1. Open http://localhost:8181

2. Use the following options to connect

The settings are in the ./postgres/docker-compose.yml file

* System: PostgreSQL
* Server: db
* Username: postgres
* Password: example
* Database: postgres

#### Create a dbt project

1. Create a new project called postgres-dbt

```
dbt init postgres-dbt
```

2. Change to the project directory

```
cd postgres-dbt
```

3. Change the name of the project in the dbt_project.yml file

#### Setup profiles

Profiles are groups of settings to connect to a directory. They are stored in
the users home directory in a sub-directory called .dbt.

1. Edit the profile file ~/.dbt/profiles.yml

2. Change settings in the profile file to connect to your database

For Postgres

https://docs.getdbt.com/reference/warehouse-profiles/postgres-profile

#### Run dbt

1. Test the settings

```
dbt debug
```

2. Generate SQL statements

```
dbt compile
```

### Presto DBT

Setup dbt for a Presto database

#### Create a dbt project

1. Create a new project called presto-dbt

```
dbt init presto-dbt
```

2. Change to project directory

```
cd presto-dbt
```

3. Change the name of the project in the dbt_project.yml file

#### Setup profiles

Profiles are groups of settings to connect to a directory. They are stored in
the users home directory in a sub-directory called .dbt.

1. Edit the profile file ~/.dbt/profiles.yml

2. Change settings in the profile file to connect to your database

For Presto

https://docs.getdbt.com/reference/warehouse-profiles/presto-profile


