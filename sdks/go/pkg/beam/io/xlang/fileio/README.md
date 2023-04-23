# Overview

```
./gradlew :sdks:java:io:file-schema-transform:expansion-service:runExpansionService
```

#### 2. Set to the latest flink runner version i.e. 1.16

```shell
FLINK_VERSION=$(./gradlew :runners:flink:properties -q --property latestFlinkVersion | grep latestFlinkVersion | cut -f 2 -d ' ')
```

#### 3. In a separate terminal, start the flink runner (It should take a few minutes on the first execution)
```shell
./gradlew :runners:flink:$FLINK_VERSION:job-server:runShadow
```
