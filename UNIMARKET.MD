# Unimarket Beam Fork

* Fork https://github.com/apache/beam
* Clone 
* Open project in intellij (as a project)
* Turn off 'Auto Import' in intellij for this project
* Configure gradle to use jdk 1.8
* You might have to run intellij as Administrator because the build tries to install go

## 2019-08-07 - ElasticsearchIO patch
* Checkout branch `release-2.14.0`
* Patch `ElasticsearchIO`
* Open `set_version.sh` and change line endings to LF
* Set the version using the following: 
```
cd <root>/unimarket-beam
./release/src/main/scripts/set_version.sh 2.14.0_UNIMARKET --release
```
* Open `BeamModulePlugin.groovy` (which has just been generated) and flip the line endings from CRLF -> LF -> CRLF
* Build module in intellij with gradle 
  `sdks > sdks-java > io > elasticsearch > Tasks > build > build`
* Run tests in intellij with gradle
 `sdks > sdks-java > io > elasticsearch-tests > Tasks > verification > test`
* Commit the two modified java source files

* On the command line 
```
cd <root>/unimarket-beam
..\..\..\..\gradlew --no-daemon -PisRelease :sdks:java:io:elasticsearch:jar
```
* Deploy to artifactory to `ext-releases-local` repo and verify the following pom data
```
<groupId>org.apache.beam</groupId>
<artifactId>beam-sdks-java-io-elasticsearch</artifactId>
<version>2.14.0_UNIMARKET</version>
```
* Create a pull request
https://github.com/apache/beam/pull/9284