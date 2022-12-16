## Запуск на локальной машине Spark и Postgres

Для запуска потребуется установленный Docker for Desktop и IntelliJ IDEA

# Spark

### Подключение к Spark через IDEA

Самый простой способ работы со Spark локально, это через IntelliJ IDEA. Нет необходимости поднимать локально, 
либо docker-контейнер со Spark. IDEA все сделает за нас. 
Создаем проект sbt (или maven), подключаем зависимости в build.sbt:

```
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"
```

Далее создаем спарк сессию и спокойно работаем со Spark:

```
val spark = SparkSession
.builder().appName("Simple Application")
.master("local[1]")
.getOrCreate()
```

# Postgres

### Подключаем зависимость для Postgres

```
libraryDependencies += "org.postgresql" % "postgresql" % "42.5.1"
```

### Docker-compose для Postgres

Создаем sql-файл для инициализации таблиц и данных:

```
docker_postgres_init.sql:

create table if not exists web (
    id int,
    timestamp bigint,
    type varchar(255),
    page_id int,
    tag varchar(255),
    sign boolean
);

create table if not exists lk (
    lk_id int,
    user_id int,
    fio varchar(255),
    birthday date,
    create_date date
    );

insert into web values
                            (12345, 1667627426, 'click', 101, 'Sport', false),
                            (12345, 1667627426, 'click', 101, 'Health', true),
                            (12347, 1667627234, 'visit', 101, 'Politic', false),
                            (12345, 1667627100, 'click', 102, 'Sport', true),
                            (12334, 1667627123, 'scroll', 102, 'Health', false),
                            (12346, 1667624555, 'click', 101, 'Politic', false),
                            (12346, 1667627429, 'visit', 98, 'Sport', true),
                            (12345, 1667620008, 'move', 100, 'Health', true);

insert into lk values
                      (1, 12345, 'Иванов Иван Иванович', '1987-04-13', '2021-03-25'),
                      (2, 12346, 'Петров Петр Петрович', '1989-05-23', '2022-01-15');
```

Создаем файл docker-compose:

```
docker-compose.yml

version: '3.8'

services:
  postgres:
    image: postgres:latest
    restart: always
    environment:
      POSTGRES_DB: "sparkdb"
      POSTGRES_USER: "spark"
      POSTGRES_PASSWORD: "spark"
    volumes:
      - postgres:/var/lib/postgresql/data
      - ./docker_postgres_init.sql:/docker-entrypoint-initdb.d/docker_postgres_init.sql
    ports:
      - "5432:5432"
volumes:
  postgres:
    driver: local
```

### Запуск контейнера с postgres

Выполняем в консоли команду:

> docker-compose up -d

Результатом станет запущеный контейнер с postgres.
Доступ к базе будет открыт через порт 5432

 - url: ***jdbc:postgresql://localhost:5432/sparkdb***
 - login/password: ***spark***/***spark***

Также будет создано 2 таблицы с данными - web и lk.

### Работа с базой с помощью Spark

создаем DataFrame'ы на основе таблиц из Postgres

```
var df_web = spark.read
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/sparkdb")
            .option("dbtable", "public.web")
            .option("user", "spark")
            .option("password", "spark")
            .load()

var df_lk = spark.read
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/sparkdb")
            .option("dbtable", "public.lk")
            .option("user", "spark")
            .option("password", "spark")
            .load()
```
Чтобы сохранить данные и создать новую таблицу на основе датафрейма выполняем код:
```
data_mart.write
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/sparkdb")
            .option("dbtable", "public.data_mart")
            .option("user", "spark")
            .option("password", "spark")
            .save()
```