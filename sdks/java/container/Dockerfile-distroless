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

# ARG BEAM_BASE is the Beam SDK container image built using sdks/python/container/Dockerfile.
ARG BEAM_BASE

# ARG DISTROLESS_BASE is the distroless container image URL. For available distroless Java images,
#   see https://github.com/GoogleContainerTools/distroless/tree/main?tab=readme-ov-file#what-images-are-available.
#   Only Java versions 17 and 21 are supported.
ARG DISTROLESS_BASE
FROM ${BEAM_BASE} AS base
ARG TARGETARCH
ENV LANG C.UTF-8

LABEL Author="Apache Beam <dev@beam.apache.org>"

RUN if [ -z "${TARGETARCH}" ]; then echo "fatal: TARGETARCH not set; run as docker buildx build or use --build-arg=TARGETARCH=amd64|arm64" >&2; exit 1; fi

FROM ${DISTROLESS_BASE}:latest-${TARGETARCH} AS distroless

COPY --from=base /opt /opt

# Along with the LANG environment variable above, prevents internally discovered failing bugs related to Dataflow Flex
# template character encodings.
COPY --from=base /usr/lib/locale /usr/lib/locale

ENTRYPOINT ["/opt/apache/beam/boot"]
