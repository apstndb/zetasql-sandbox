```
$ gradle jibDockerBuild --image=zetasql-sandbox
$ docker run -i --rm zetasql-sandbox format < input.sql
```

Query analysis using actual table metadata.
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
