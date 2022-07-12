# Draethos

![Draethos](docs/draethos-jedi.jpg)

Following the end of the Great Hyperspace War, Jedi Knight Odan-Urr, a noted Jedi Consular and Lore Keeper returned to the planet of his early training in the Jedi Order under Jedi Master Ooroo. Drawing inspiration from the expansive library found in the Praxeum in Knossa, Odan-Urr began to conceptualize a massive archive of wisdom on the quiet and meditative world of Ossus.

From the days of its foundation, the Library of Ossus contained the culmination of hundreds of generations of knowledge and wisdom. Odan-Urr scoured the galaxy for all attainable knowledge, eventually cataloging what was said to be data on all sentient works and history from the foundation of the Galactic Republic. Containing rare scrolls and hand-written books, many librarians and archivists recorded these text in more secure datacrons and holodiscs in order to ensure their preservation and integrity.

## Description 

Tool developed to work as a utility, providing an easy way to populate databases from events.

It is possible to use as a utility, having compilation for linux, macos and windows, and also being possible to run through container orchestrators.

## Build project
```sh
make docker-build release=latest
```

## Draethos Cli.

```sh
 ____                 _   _               
|  _ \ _ __ __ _  ___| |_| |__   ___  ___ 
| | | | '__/ _' |/ _ \ __| '_ \ / _ \/ __|
| |_| | | | (_| |  __/ |_| | | | (_) \__ \
|____/|_|  \__,_|\___|\__|_| |_|\___/|___/
                           release: latest

Usage:
  draethos [command]

Available Commands:
  generate    Generage scaffold
  help        Help about any command
  start       Start application

Flags:
  -h, --help      help for draethos
  -v, --version   version for draethos

Use "draethos [command] --help" for more information about a command.
```

### Generate Scaffold

As an option to create a new stream it is possible to use the scaffold generator, below is an example.

```sh
./draethos generate \
        --port 8000 \
        --export-path ./share/pipeline.yaml \
        --instance.source.type kafka \
        --instance.source.specs.topic topic.source \
        --instance.source.specs.configurations "group.id=draethos" \
        --instance.source.specs.configurations "bootstrap.servers=localhost:9093" \
        --instance.source.specs.configurations "auto.offset.reset=beginning" \
        --instance.target.type topic.target \
        --instance.target.specs.batchSize 100 \
        --instance.target.specs.configurations "bootstrap.servers=localhost:9093"
``` 

### Execute stream

To initialize an instance just run the command below, it is also possible to initialize with health check and prometheus metrics if you want to run within a container orchestrator.

```sh 
./draethos start -f pipeline.yaml -l -m -p 9999
``` 

### Docker Container Example

Below is an example of how to work with draethos using container.

```Dockerfile
FROM adrianolaselva/draethos:latest

COPY ./share .

ENTRYPOINT ["./draethos", "-f", "./share/pipeline.yaml", "-l", "-m"]
```

## Pipelines.

Available connectors

### Sources

| Id    | Source       |
|-------|--------------|
| kafka | Apache Kafka |
| http  | HTTP request |

### Targets

| Id    | Target       |
|-------|--------------|
| kafka | Apache Kafka |
| s3    | AWS S3       |
| sqs   | AWS SQS      |
| sns   | AWS SNS      |
| pgsql | Postgres     |
| mysql | Mysql        |

## References

- [golang-standards](https://github.com/golang-standards/project-layout)
- [Kafka](https://kafka.apache.org/)
- [Postgres](https://www.postgresql.org/)
- [Mysql](https://www.mysql.com/)
- [AWS s3](https://aws.amazon.com/pt/s3/)
- [AWS sns](https://aws.amazon.com/pt/sns/)
- [AWS sqs](https://aws.amazon.com/pt/sqs/)