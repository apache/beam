#!/usr/bin/env bash

if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" = 'false' ]; then
    openssl aes-256-cbc -K $encrypted_86a2d9f3b234_key -iv $encrypted_86a2d9f3b234_iv -in cd/code-signing.asc.enc -out cd/code-signing.asc -d
    gpg --fast-import cd/code-signing.asc
fi
