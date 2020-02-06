```
$ gradle jibDockerBuild --image=zetasql-sandbox
$ docker run --rm zetasql-sandbox 'SELECT * FROM (SELECT 1, 2, "test" AS x)' 
QueryStmt
$col1:INT64,$col2:INT64,x:STRING
```
