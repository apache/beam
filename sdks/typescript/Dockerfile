###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
###############################################################################

FROM node:16
MAINTAINER "Apache Beam <dev@beam.apache.org>"

ENV NODE_ENV=development

WORKDIR /app

# Copy beam package.
COPY apache-beam-0.38.0.tgz ./apache-beam-0.38.0.tgz

# Install dependencies and compile
RUN npm install apache-beam-0.38.0.tgz

# Check that filesystem is set up as expected
RUN ls -a

COPY boot /opt/apache/beam/
ENTRYPOINT ["/opt/apache/beam/boot"]
