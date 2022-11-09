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

import java.io.FileOutputStream

description = "Apache Beam :: Playground :: playground_components Flutter Package"

tasks.register("configure") {
  dependsOn("generateCode")
  dependsOn("extractBeamSymbols")

  group = "build"
  description = "After checkout, gets everything ready for local development, test, or build."
}

tasks.register("precommit") {
  dependsOn("analyze")
  dependsOn("test")

  group = "verification"
  description = "All possible checks before a commit."
}

tasks.register("analyze") {
  dependsOn("generateCode")

  group = "verification"
  description = "Run dart analyzer"

  doLast {
    exec {
      executable("dart")
      args("analyze", ".")
    }
  }
}

tasks.register("test") {
  dependsOn("generateCode")

  group = "verification"
  description = "Run tests"

  doLast {
    exec {
      executable("flutter")
      args("test")
    }
  }
}

tasks.register("cleanFlutter") {
  group = "build"
  description = "Remove build artifacts"

  doLast {
    exec {
      executable("flutter")
      args("clean")
    }
  }
}

tasks.register("pubGet") {
  group = "build"
  description = "Install dependencies"

  doLast {
    exec {
      executable("flutter")
      args("pub", "get")
    }
  }
}

tasks.register("generateCode") {
  dependsOn("cleanFlutter")
  dependsOn("pubGet")

  group = "build"
  description = "Generate code"

  doLast {
    exec {
      executable("flutter")
      args("pub", "run", "build_runner", "build", "--delete-conflicting-outputs")
    }
  }
}

tasks.register("extractBeamSymbols") {
  dependsOn("ensureSymbolsDirectoryExists")
  dependsOn("extractBeamSymbolsPython")

  group = "build"
  description = "Build Beam symbols dictionaries"
}

tasks.register("ensureSymbolsDirectoryExists") {
  doLast {
    exec {
      executable("mkdir")
      args("-p", "assets/symbols")
    }
  }
}

tasks.register("extractBeamSymbolsPython") {
  doLast {
    exec {
      executable("python3")
      args(
        "tools/extract_symbols_python/extract_symbols_python.py",
        "../../../sdks/python/apache_beam",
      )
      standardOutput = FileOutputStream("playground/frontend/playground_components/assets/symbols/python.g.yaml")
    }
  }
}
