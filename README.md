It is personal toy project using official Java wrapper of [ZetaSQL](https://github.com/google/zetasql) (Google's SQL dialect parser & analyzer) by Kotlin.
Officially, ZetaSQL supports only Linux so it should be run on Docker.

## build

```
$ ./gradlew jibDockerBuild --image=zetasql-sandbox
```

### format

It format query from standard input.

```
$ echo "SELECT * FROM table" | docker run -i --rm zetasql-sandbox format
```

from file

```
$ docker run -i --rm zetasql-sandbox format < input.sql
```

### extract-table

It extract referenced table from standard input.

```
$ echo 'SELECT actor_attributes.* FROM `bigquery-public-data.samples.github_nested`' | docker run -i --rm zetasql-sandbox extract-table
bigquery-public-data.samples.github_nested
```

### analyze-print-with-bqschema

Query analysis using actual table metadata. It use GCP credentials by ADC.

Use gcloud credentials

```
$ docker run -i --rm --volume ${HOME}/.config/gcloud:/root/.config/gcloud zetasql-sandbox analyze-print-with-bqschema \
             <<< 'SELECT actor_attributes.* FROM `bigquery-public-data.samples.github_nested`'           
SELECT
  `bigquery-public-data.samples.github_nested_2`.a_1.blog AS blog,
  `bigquery-public-data.samples.github_nested_2`.a_1.company AS company,
  `bigquery-public-data.samples.github_nested_2`.a_1.email AS email,
  `bigquery-public-data.samples.github_nested_2`.a_1.gravatar_id AS gravatar_id,
  `bigquery-public-data.samples.github_nested_2`.a_1.location AS location,
  `bigquery-public-data.samples.github_nested_2`.a_1.login AS login,
  `bigquery-public-data.samples.github_nested_2`.a_1.name AS name,
  `bigquery-public-data.samples.github_nested_2`.a_1.type AS type
FROM
  (
    SELECT
      `bigquery-public-data.samples.github_nested`.actor_attributes AS a_1
    FROM
      `bigquery-public-data.samples.github_nested`
  ) AS `bigquery-public-data.samples.github_nested_2`;
```

## Limitation

Doesn't yet support:
* Table wildcard(`_TABLE_SUFFIX`)
* and more
