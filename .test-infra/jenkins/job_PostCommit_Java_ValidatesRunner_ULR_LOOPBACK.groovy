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

import CommonJobProperties as commonJobProperties
import PostcommitJobBuilder

String prTriggerPhrase = './gradlew :runners:portability:java:ulrLoopbackValidatesRunner'

// This job runs the suite of ValidatesRunner tests against the Direct
// runner compiled with Java 8.
PostcommitJobBuilder.postCommitJob('beam_PostCommit_Java_ValidatesRunner_ULR',
        prTriggerPhrase,
        "Universal Local Runner ValidatesRunner Tests for Java, LOOPBACK mode (${prTriggerPhrase})", this) {

      description('Builds the Universal Local Runner and runs the Java ValidatesRunner test suite in LOOPBACK mode (no Docker).')

      commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 180)

      publishers {
        archiveJunit('**/build/test-results/**/*.xml')
      }

      steps {
        gradle {
          rootBuildScriptDir(commonJobProperties.checkoutDir)
          tasks(':runners:portability:java:ulrLoopbackValidatesRunner')
        }
      }

    }
