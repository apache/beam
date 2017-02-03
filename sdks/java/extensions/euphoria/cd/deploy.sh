#!/usr/bin/env bash

if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" = 'false' ]; then
    mvn deploy -DskipTests=true -P sign --settings cd/mvn-settings.xml
fi
