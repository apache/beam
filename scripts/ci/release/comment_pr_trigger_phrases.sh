#!/bin/bash
#file="./jenkins_trigger_phrases.txt"
file="./example.txt"
GITHUB_PR=$1
while IFS= read -r trigger_phrase
do
    #printf '%s\n' "$trigger_phrase"
    gh pr comment "$GITHUB_PR" --body "$trigger_phrase"
done <"$file"