## Запускать consumer и producer просто из idea как main()

- топики создать с именем nvdenisov-hse-2023 и nvdenisov-hse-2023-processed
- consumer_1 читает из первого топика. consumer_2 читает из второго. producer пишет в первый
- настроил flink checkpoint в локальную директорию (скрины в репо)
- tumling и session window работают ок. sliding window у меня почему-то висит в интерфейсе flink джоба и постоянно пересоздается — не смог отдебагать(
- Сорри, что просрочил дедлайн. Надеюсь не повлиял на проверку работ... (Надеюсь на 45 * 0.7 = 31.5 баллов)

### Kafka


```commandline
docker-compose build
```


```commandline
docker-compose up -d
```

```commandline
docker-compose ps
```
```
http://localhost:8081/#/overview

```
```commandline
docker-compose down -v
```

```commandline
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic topic_name --partitions 1 --replication-factor 1
```
```commandline
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --describe itmo  
```
```commandline
 docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --alter --topic itmo --partitions 2

```

```commandline
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job.py -d  
```
