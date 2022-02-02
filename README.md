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

## Pipelines.

### Sources
|  Id |  Source |
|---|---|
| kafka  | Apache Kafka  |

### Targets
|  Id |  Target |
|---|---|
| kafka  | Apache Kafka  |
| s3  | AWS S3  |
| sqs  | AWS SQS  |
| sns | AWS SNS  |
| pgsql  | Postgres  |
| mysql  | Mysql  |
