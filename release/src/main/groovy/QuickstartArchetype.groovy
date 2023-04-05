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

class QuickstartArchetype {
  def static generate(TestScripts t) {
    // Generate a maven project from the snapshot repository
    String output_text = t.run """mvn archetype:generate \
      --update-snapshots \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
      -DarchetypeVersion=${t.ver()} \
      -DgroupId=org.example \
      -DartifactId=word-count-beam \
      -Dversion="0.1" \
      -Dpackage=org.apache.beam.examples \
      -DinteractiveMode=false"""

    // Check if it was generated
    t.see "[INFO] BUILD SUCCESS", output_text
    t.run "cd word-count-beam"
    output_text = t.run "ls"
    t.see "pom.xml", output_text
    t.see "src", output_text
    String wordcounts = t.run "ls src/main/java/org/apache/beam/examples/"
    t.see "WordCount.java", wordcounts
    String games = t.run "ls src/main/java/org/apache/beam/examples/complete/game/"
    t.see "UserScore.java", games
  }
}
