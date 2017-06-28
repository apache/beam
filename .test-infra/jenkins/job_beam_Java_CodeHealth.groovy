/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import common_job_properties

// This is the Java Jenkins job which runs the Beam code health checks.
mavenJob('beam_Java_CodeHealth') {
  description('Runs Java code health checks. Meant to be run as part of a pipeline.')

  // Set standard properties for a job which is part of a pipeline.
  common_job_properties.setPipelineJobProperties(delegate, 15, "Java Code Health")
  // This job runs downstream of the beam_Java_Build job and gets artifacts from that job.
  common_job_properties.setPipelineDownstreamJobProperties(delegate, 'beam_Java_Build')

  args = [
    '-B',
    '-e',
    "-pl '!sdks/python'",
    'checkstyle:check',
    'findbugs:check',
    'org.apache.rat:apache-rat-plugin:check',
  ]
  goals(args.join(' '))
}
