<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->
# Running script locally
You can either run it via docker-compose that is located in .test-infra/metrics/
or run manually via docker. Below is insturctions for running script with
docker.

1. Build container `docker build -t syncjenkins .`
2. Edit script to initialize db host via subprocess.
3. `docker run -it --rm --name sync -v "$PWD":/usr/src/myapp -w /usr/src/myapp -e "DB_PORT=5432" -e "DB_DBNAME=beam_metrics" -e "DB_DBUSERNAME=admin" -e "DB_DBPWD=<password>" syncjira python syncjira.py`

