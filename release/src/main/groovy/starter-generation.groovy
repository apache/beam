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
 * Generate a starter archetype project and ensure its correctness
 */

t.describe 'Generate project from starter archetype'

t.intent 'Generates starter archetype project'
StarterArchetype.generate(t)

t.intent 'Runs the StarterPipeline Code with Direct runner'
// Run the wordcount example with the Direct runner
t.run """mvn compile exec:java -q \
      -Dexec.mainClass=org.apache.beam.starter.StarterPipeline \
      -Dexec.args="--inputFile=pom.xml --output=starterOutput"""

// Verify output correctness
String result = t.run "grep INFO starterOutput*"
t.see "INFO: HELLO", result
t.see "INFO: WORLD", result

// Clean up
t.done()
