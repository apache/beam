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

description = "Apache Beam :: Playground :: Backend"

task("format") {
  group = "build"
  description = "Format backend go code"
  doLast {
    exec {
      executable("gofmt")
      args("-w", ".")
    }
  }
}

task("tidy") {
  group = "build"
  description = "Run go mod tidy"
  doLast {
    exec {
      executable("go")
      args("mod", "tidy")
    }
  }
}

task("test") {
  group = "verification"
  description = "Test the backend"
  doLast {
    exec {
      executable("go")
      args("test", "./...")
    }
  }
}

task("benchmarkPrecompiledObjects") {
  group = "verification"
  description = "Run benchmarks for precompiled objects"
  doLast {
    exec {
      executable("go")
      args("test", "-bench", ".", "-benchmem", "./internal/cloud_bucket/...")
    }
  }
}

task("benchmarkCodeProcessing") {
  group = "verification"
  description = "Run benchmarks for code processing"
  doLast {
    exec {
      executable("go")
      args("test", "-run=^$", "-bench", ".", "-benchmem", "./internal/code_processing/...")
    }
  }
}

task("benchmark") {
  dependsOn(":playground:backend:benchmarkPrecompiledObjects")
  dependsOn(":playground:backend:benchmarkCodeProcessing")
}


task("installLinter") {
  doLast {
    exec {
      executable("sh")
      args("env_setup.sh")
    }
  }
}

task("runLint") {
  dependsOn(":playground:backend:installLinter")
  doLast {
    exec {
      executable("golangci-lint")
      args("run", "cmd/server/...")      
    }
  }
}

task("precommit") {
  dependsOn(":playground:backend:runLint")
  dependsOn(":playground:backend:tidy")
  dependsOn(":playground:backend:test")
  dependsOn(":playground:backend:benchmark")
}

