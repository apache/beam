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

ARG BASE
FROM ${BASE} AS base
ENV LANG=C.UTF8
ARG TARGETARCH
LABEL Author="Apache Beam <dev@beam.apache.org>"

RUN if [ -z "${TARGETARCH}" ]; then echo "fatal: TARGETARCH not set; run as docker buildx build or use --build-arg=TARGETARCH=amd64|arm64" >&2; exit 1; fi

FROM gcr.io/distroless/python3-debian12:latest-${TARGETARCH} AS distroless

# Prevents internal errors found with distroless container images and Flex templates.
COPY --from=base /usr/lib/locale /usr/lib/locale

# Contains header files needed by the Python interpreter.
COPY --from=base /usr/local/include /usr/local/include

# Contains the Python interpreter executables.
COPY --from=base /usr/local/bin /usr/local/bin

# Contains the Python library dependencies.
COPY --from=base /usr/local/lib /usr/local/lib

# Python standard library modules.
COPY --from=base /usr/lib/python* /usr/lib/.

# Contains the boot entrypoint and related files such as licenses.
COPY --from=base /opt /opt

ENV PATH "$PATH:/usr/local/bin"

# Despite the ENTRYPOINT set in base image, need to reset since deriving the layer derives from a different image.
ENTRYPOINT ["/opt/apache/beam/boot"]
