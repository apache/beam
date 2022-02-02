# Integration Test Utilities

## What Is Included

Anything that can be used to aid writing integration tests for either Classic
or Flex Templates.

## What Is Not Included

The integration tests themselves. Those should go in the test folder for the
template that they are being written for.

## Running A Test

NOTE: All commands are run from the root of the repository.

Build this library:

```shell
mvn clean install -f it/pom.xml
```

Authorize yourself in Google Cloud and set the required access token variable:

```shell
gcloud auth application-default login
export DT_IT_ACCESS_TOKEN=$(gcloud auth application-default print-access-token)
```

Run the test (using a Flex Template):

```shell
mvn clean test -f v2/pom.xml -pl "$MODULE" \
 -Dtest="$TEST_CLASS#$TEST_METHOD" \
 -Dproject="$PROJECT" \
 -DartifactBucket="$BUCKET" \
 -Dregion="$REGION" \
 -DspecPath="$SPEC_PATH"
```

NOTE: All the above args are required except `-Dregion`. If it is not provided,
then the template will run in `us-central1`.
