#!/bin/bash
file="./mass_comment.txt"
GITHUB_PR=$1
while IFS= read -r trigger_phrase
do
    gh pr comment "$GITHUB_PR" --body "$trigger_phrase"
done <"$file"
