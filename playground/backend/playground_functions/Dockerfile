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

# This Dockerfile is only for local testing

FROM golang:1-alpine as build

COPY . /app
WORKDIR /app/playground_functions

RUN ls -la

RUN go mod download

RUN go build -o /app/cloudfunction ./cmd

FROM alpine:3.17

COPY --from=build /app/cloudfunction /app/cloudfunction

EXPOSE 8080

ENTRYPOINT ["/app/cloudfunction"]
