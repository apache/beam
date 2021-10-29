/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

description = "Apache Beam :: Playground :: Frontend"

task("analyze") {
  group = "verification"
  description = "Analyze dart code"
  doLast {
    exec {
      executable("dart")
      args("analyze", ".")
    }
  }
  mustRunAfter("pubGet")
}

task("pubGet") {
  group = "build"
  description = "Get packages for the playground frontend project"
  doLast {
    exec {
      executable("flutter")
      args("pub", "get")
    }
  }
}

task("format") {
  group = "build"
  description = "Idiomatically format Dart source code"
  doLast {
    exec {
      executable("dart")
      args("format", ".")
    }
  }
}

task("run") {
  group = "application"
  description = "Run application on Google Chrome"
  doLast {
    exec {
      executable("flutter")
      args("run", "-d", "chrome")
    }
  }
}

task("test") {
  group = "verification"
  description = "flutter test"
  doLast {
    exec {
      executable("flutter")
      args("test")
    }
  }
}