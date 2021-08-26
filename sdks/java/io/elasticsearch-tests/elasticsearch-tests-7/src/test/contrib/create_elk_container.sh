#!/bin/sh
################################################################################
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
################################################################################

#Create an ELK (Elasticsearch Logstash Kibana) container for ES v6.4.0 and compatible Logstash and Kibana versions,
#bind then on host ports, allow shell access to container and mount current directory on /home/$USER inside the container

docker create -p 5601:5601 -p 9200:9200 -p 5044:5044 -p 5000:5000 -p 9300:9300 -it -v $(pwd):/home/$USER/ --name elk-7.5.0  sebp/elk:740
