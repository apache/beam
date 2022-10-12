# Cosmos DB Core SQL API

Compile all module azure-cosmosdb

```shell
gradle sdks:java:io:azure-cosmosdb:build
```

## Test

Run TEST for this module (Cosmos DB Core SQL API):

```shell
gradle sdks:java:io:azure-cosmosdb:test
```


## Publish in Maven Local

Publish this module 

```shell
# apache beam core
gradle -Ppublishing -PdistMgmtSnapshotsUrl=~/.m2/repository/ -p sdks/java/core/  publishToMavenLocal

# apache beam azure-cosmosdb
gradle -Ppublishing -PdistMgmtSnapshotsUrl=~/.m2/repository/ -p sdks/java/io/azure-cosmosdb/  publishToMavenLocal
```

Publish all modules of apache beam

```shell
gradle -Ppublishing -PdistMgmtSnapshotsUrl=~/.m2/repository/ -p sdks/java/  publishToMavenLocal

gradle -Ppublishing -PdistMgmtSnapshotsUrl=~/.m2/repository/ -p runners/  publishToMavenLocal

gradle -Ppublishing -PdistMgmtSnapshotsUrl=~/.m2/repository/ -p model/ publishToMavenLocal
```


