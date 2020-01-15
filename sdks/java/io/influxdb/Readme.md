To test the influxdb io plugin. Run the influxDB container.
docker run -p 8086:8086 -p 2003:2003  -p 8083:8083    -e INFLUXDB_GRAPHITE_ENABLED=true -e INFLUXDB_USER=supersadmin -e INFLUXDB_USER_PASSWORD=supersecretpassword  influxdb


./gradlew -Ppublishing -PdistMgmtSnapshotsUrl=~/.m2/repository/ -p sdks/java/io/influxdb publishToMavenLocal
./gradlew integrationTest -p sdks/java/io/influxdb -DintegrationTestPipelineOptions='[ "--influxDBURL=http://localhost:8086", "--influxDBUserName=superadmin",  "--influxDBPassword=supersecretpassword", "--databaseName=db1" ]' --tests org.apache.beam.sdk.io.influxdb.InfluxDBIOIT  -DintegrationTestRunner=direct