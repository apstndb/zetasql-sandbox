```
$ gradle jibDockerBuild --image=zetasql-sandbox
$ docker run -i --rm zetasql-sandbox format < input.sql
```
