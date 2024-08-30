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

# Beam YAML API

The Beam YAML API provides a simple declarative syntax for describing pipelines
that does not require coding experience or learning how to use an
SDK&mdash;any text editor will do.
Some installation may be required to actually *execute* a pipeline, but
we envision various services (such as Dataflow) to accept yaml pipelines
directly obviating the need for even that in the future.
We also anticipate the ability to generate code directly from these
higher-level yaml descriptions, should one want to graduate to a full
Beam SDK (and possibly the other direction as well as far as possible).

Though we intend this syntax to be easily authored (and read) directly by
humans, this may also prove a useful intermediate representation for
tools to use as well, either as output (e.g. a pipeline authoring GUI)
or consumption (e.g. a lineage analysis tool) and expect it to be more
easily manipulated and semantically meaningful than the Beam protos
themselves (which concern themselves more with execution).

## More details

User-facing documentation for Beam YAML has moved to the main Beam site at
https://beam.apache.org/documentation/sdks/yaml/

For information about contributing to Beam YAML see
https://docs.google.com/document/d/19zswPXxxBxlAUmswYPUtSc-IVAu1qWvpjo1ZSDMRbu0
