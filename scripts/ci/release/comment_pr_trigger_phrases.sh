#!/bin/bash
file="./jenkins_trigger_phrases.txt"
GITHUB_PR=$1
while IFS= read -r trigger_phrase
do
    gh pr comment "$GITHUB_PR" --body "$trigger_phrase"
done <"$file"
