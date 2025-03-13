#!/bin/bash
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
# This script adds comments from a txt file, in the given GitHub Pull Request.
# test/resources/mass_comment.txt file has the Trigger Phrases for running Gradle Build and PostCommit/PreCommit tests.

file="./test/resources/mass_comment.txt"
GITHUB_PR=$1
while IFS= read -r trigger_phrase
do
    gh pr comment "$GITHUB_PR" --body "$trigger_phrase"
    sleep 30
done <"$file"
