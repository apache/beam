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

Start-Sleep -Seconds 30
$env:Path += ';C:\Program Files\git\bin'
$response= Invoke-RestMethod https://api.github.com/repos/actions/runner/tags
$version= $response[0].name.substring(1,$response[0].name.Length-1)

$ORG_NAME="apache"
$ORG_RUNNER_GROUP="Beam"
$GCP_TOKEN=gcloud auth print-identity-token
$TOKEN_PROVIDER="https://$GCP_REGION-$GCP_PROJECT_ID.cloudfunctions.net/$CLOUD_FUNCTION_NAME" #Replace variables manually

Set-Location C:/

Write-Output "Starting registration process"

mkdir "actionsDir"

Set-Location "actionsDir"

Invoke-WebRequest -Uri https://github.com/actions/runner/releases/download/v$version/actions-runner-win-x64-$version.zip -OutFile actions-runner-win-x64-$version.zip

Expand-Archive -LiteralPath $PWD\actions-runner-win-x64-$version.zip -DestinationPath $PWD -Force

$RUNNER_TOKEN=(Invoke-WebRequest -Uri $TOKEN_PROVIDER -Method POST -Headers @{'Accept' = 'application/json'; 'Authorization' = "bearer $GCP_TOKEN"} -UseBasicParsing | ConvertFrom-Json).token

[System.Environment]::SetEnvironmentVariable('GITHUB_TOKEN', $RUNNER_TOKEN,[System.EnvironmentVariableTarget]::Machine)

$hostname= "windows-runner-"+[guid]::NewGuid()

./config.cmd --name $hostname --token $RUNNER_TOKEN --url https://github.com/$ORG_NAME --work _work --unattended --replace --labels windows,beam,windows-server-2019 --runnergroup $ORG_RUNNER_GROUP

./run.cmd