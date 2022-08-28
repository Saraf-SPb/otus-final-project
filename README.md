# Выпускной проект OTUS "Spark Developer"
https://otus.ru/lessons/spark/

За основу взят репозиторий https://github.com/vadopolski/otus-hadoop-homework

Генератор событий поставляет в Kafka статистику посещения сайта.

## SpeedLayer
* С определенной периодичностью собирается статистика откуда перешел пользователь на ту или иную страницу и общее количество посещений и записывается в виде Parquet.

* Исходные данные приземляются в Cassandra без преобразований.

## BatchLayer
* Собранные stream-слое данные аккумулируются и считается статистика за все время, хранится в HDFS в ORC-формате партиционированна по дате отчета.


Доработки:

* добавлены sub-проекты
* добавлена Cassandra для speed-слоя
* для batch-слоя изменен формат хранения данных с parquet на ORC
* На batch-слое добавлено партиционирование
* очищен мусор в коде
* добавлен assembly-plugin