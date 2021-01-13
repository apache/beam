#!groovy
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

t = new TestScripts(args)

/*
 * Run the Twister2 quickstart from https://beam.apache.org/get-started/quickstart-java/
 */

t.describe 'Run Apache Beam Java SDK Quickstart - Twister2'

  t.intent 'Gets the WordCount Example Code'
    QuickstartArchetype.generate(t)

  t.intent 'Runs the WordCount Code with Spark runner'
    // Run the wordcount example with the Twister2 runner
    t.run """mvn compile exec:java -q \
      -Dexec.mainClass=org.apache.beam.examples.WordCount \
      -Dexec.args="--inputFile=pom.xml --output=counts \
      --runner=Twister2Runner" -Ptwister2-runner"""

    // Verify text from the pom.xml input file
    String result = t.run "grep Foundation counts*"
    t.see "Foundation: 1", result

    // Clean up
    t.done()
