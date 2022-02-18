# <!--
#     Licensed to the Apache Software Foundation (ASF) under one
#     or more contributor license agreements.  See the NOTICE file
#     distributed with this work for additional information
#     regarding copyright ownership.  The ASF licenses this file
#     to you under the Apache License, Version 2.0 (the
#     "License"); you may not use this file except in compliance
#     with the License.  You may obtain a copy of the License at

#       http://www.apache.org/licenses/LICENSE-2.0

#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
# -->

$env:Path += ';C:\Program Files\git\bin' 
  

$response= Invoke-RestMethod https://api.github.com/repos/actions/runner/tags
$version= $response[0].name.substring(1,$response[0].name.Length-1)
Set-Location C:/

mkdir "actionsDir"    

Set-Location "actionsDir"

Invoke-WebRequest -Uri https://github.com/actions/runner/releases/download/v$version/actions-runner-win-x64-$version.zip -OutFile actions-runner-win-x64-$version.zip

Expand-Archive -LiteralPath $PWD\actions-runner-win-x64-$version.zip -DestinationPath $PWD -Force

$GITHUB_ORG=gcloud secrets versions access latest --secret="ORG_SECRET"
$GITHUB_RUNNER_GROUP=gcloud secrets versions access latest --secret="GROUP_SECRET" 
$GITHUB_TOKEN=gcloud secrets versions access latest --secret="TOKEN_SECRET"    


Write-Output "Starting registration process"

$registration_url="https://api.github.com/orgs/$GITHUB_ORG/actions/runners/registration-token"
Write-Output "Requesting registration URL at '${registration_url}'"



$payload= Invoke-WebRequest ${registration_url} -UseBasicParsing -Method 'POST' -Headers @{'Authorization'="token $GITHUB_TOKEN"} | ConvertFrom-Json
[System.Environment]::SetEnvironmentVariable('GITHUB_TOKEN', $payload.token,[System.EnvironmentVariableTarget]::Machine)
Write-Output $payload.token | Out-File "token.txt"

$hostname= "windows-runner-"+[guid]::NewGuid()

./config.cmd --name $hostname --token $payload.token --url https://github.com/$GITHUB_ORG --runnergroup $GITHUB_RUNNER_GROUP --work _work --unattended --replace --labels windows,windows-latest,windows-server-2019

./run.cmd