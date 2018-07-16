#!/bin/bash
curl https://cwiki.apache.org/confluence/display/INFRA/Jenkins+Plugin+Upgrades > plugins.txt
sed -i ':a;N;$!ba;s/\n/ /g' plugins.txt
grep -Po -m 1 '<h3 id="JenkinsPluginUpgrades-.*?</table>' plugins.txt | head -1 > plugins1.txt
mv -f plugins1.txt plugins.txt
sed -i 's/.*<tbody>\(.*\)<\/tbody>.*/\1/' plugins.txt
sed -i 's/<\/tr>/\n/g' plugins.txt
sed -i 's/.*<tr>//g' plugins.txt
tail -n +2 plugins.txt > plugins1.txt
mv -f plugins1.txt plugins.txt
sed -i 's/<td.*><a.*>\(.*\)<\/a><\/td><td.*\/td><td.*\/td><td.*>\(.*\)<\/td><td.*\/td>/\1:\2/g' plugins.txt

