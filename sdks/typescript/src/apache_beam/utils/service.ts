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

const fs = require("fs");
const https = require("https");
const os = require("os");
const net = require("net");
const path = require("path");
const childProcess = require("child_process");

// TODO: (Typescript) Why can't the var above be used as a namespace?
import { ChildProcess } from "child_process";

import { version as beamVersion } from "../version";

export interface Service {
  start: () => Promise<string>;
  stop: () => Promise<void>;
}

export class ExternalService implements Service {
  constructor(public address: string) {
    this.address = address;
  }
  async start() {
    return this.address;
  }
  async stop() {}
}

export class SubprocessService {
  process: ChildProcess;
  cmd: string;
  args: string[];

  constructor(cmd: string, args: string[]) {
    this.cmd = cmd;
    this.args = args;
  }

  async start() {
    // TODO: (Cleanup) Choose a free port.
    const host = "localhost";
    const port = "7778";
    console.log(this.args.map((arg) => arg.replace("{{PORT}}", port)));
    this.process = childProcess.spawn(
      this.cmd,
      this.args.map((arg) => arg.replace("{{PORT}}", port)),
      {
        stdio: "inherit",
      }
    );

    try {
      await this.portReady(port, host, 10000);
    } catch (error) {
      this.process.kill();
      throw error;
    }

    return host + ":" + port;
  }

  async stop() {
    this.process.kill();
  }

  async portReady(port, host, timeoutMs, iterMs = 100) {
    const start = Date.now();
    let connected = false;
    while (!connected && Date.now() - start < timeoutMs) {
      if (this.process.exitCode) {
        throw new Error("Aborted with error " + this.process.exitCode);
      }
      await new Promise((r) => setTimeout(r, iterMs));
      try {
        await new Promise<void>((resolve, reject) => {
          const socket = net.createConnection(port, host, () => {
            resolve();
            socket.end();
            connected = true;
          });
          socket.on("error", (err) => {
            reject(err);
          });
        });
      } catch (err) {
        // go around again
      }
    }
    if (!connected) {
      throw new Error(
        "Timed out waiting for service after " + timeoutMs + "ms."
      );
    }
  }
}

export function serviceProviderFromJavaGradleTarget(
  gradleTarget: string,
  args: string[] | undefined = undefined
): () => Promise<JavaJarService> {
  return async () => {
    return new JavaJarService(
      await JavaJarService.cachedJar(JavaJarService.gradleToJar(gradleTarget)),
      args
    );
  };
}

export class JavaJarService extends SubprocessService {
  static APACHE_REPOSITORY = "https://repo.maven.apache.org/maven2";
  static BEAM_GROUP_ID = "org.apache.beam";
  static JAR_CACHE = path.join(os.homedir(), ".apache_beam", "cache", "jars");

  constructor(jar: string, args: string[] | undefined = undefined) {
    if (args == undefined) {
      // TODO: (Extension) Should filesToStage be set at some higher level?
      args = ["{{PORT}}", "--filesToStage=" + jar];
    }
    super("java", ["-jar", jar].concat(args));
  }

  static async cachedJar(
    urlOrPath: string,
    cacheDir: string = JavaJarService.JAR_CACHE
  ): Promise<string> {
    if (urlOrPath.match(/^https?:\/\//)) {
      fs.mkdirSync(cacheDir, { recursive: true });
      const dest = path.join(
        JavaJarService.JAR_CACHE,
        path.basename(urlOrPath)
      );
      if (fs.existsSync(dest)) {
        return dest;
      }
      // TODO: (Cleanup) Use true temporary file.
      const tmp = dest + ".tmp" + Math.random();
      return new Promise((resolve, reject) => {
        const fout = fs.createWriteStream(tmp);
        console.log("Downloading", urlOrPath);
        const request = https.get(urlOrPath, function (response) {
          if (response.statusCode !== 200) {
            reject(
              `Error code ${response.statusCode} when downloading ${urlOrPath}`
            );
          }
          response.pipe(fout);
          fout.on("finish", function () {
            fout.close(() => {
              fs.renameSync(tmp, dest);
              resolve(dest);
            });
          });
        });
      });
    } else {
      return urlOrPath;
    }
  }

  static gradleToJar(
    gradleTarget: string,
    appendix: string | undefined = undefined,
    version: string = beamVersion
  ): string {
    if (version.startsWith("0.")) {
      // node-ts 0.x corresponds to Beam 2.x.
      version = "2" + version.substring(1);
    }
    const gradlePackage = gradleTarget.match(/^:?(.*):[^:]+:?$/)![1];
    const artifactId = "beam-" + gradlePackage.replaceAll(":", "-");
    // TODO: Do this more robustly, e.g. use the git root.
    const projectRoot = path.resolve(
      __dirname,
      "..",
      "..",
      "..",
      "..",
      "..",
      ".."
    );
    const localPath = path.join(
      projectRoot,
      gradlePackage.replaceAll(":", path.sep),
      "build",
      "libs",
      JavaJarService.jarName(
        artifactId,
        version.replace(".dev", ""),
        "SNAPSHOT",
        appendix
      )
    );

    if (fs.existsSync(localPath)) {
      console.log("Using pre-built snapshot at", localPath);
      return localPath;
    } else if (version.includes(".dev")) {
      throw new Error(
        `${localPath} not found. Please build the server with
      cd ${projectRoot}; ./gradlew ${gradleTarget})`
      );
    } else {
      return JavaJarService.mavenJarUrl(
        artifactId,
        version,
        undefined,
        appendix
      );
    }
  }

  static mavenJarUrl(
    artifactId: string,
    version: string,
    classifier: string | undefined = undefined,
    appendix: string | undefined = undefined,
    repo: string = JavaJarService.APACHE_REPOSITORY,
    groupId: string = JavaJarService.BEAM_GROUP_ID
  ): string {
    return [
      repo,
      groupId.replaceAll(".", "/"),
      artifactId,
      version,
      JavaJarService.jarName(artifactId, version, classifier, appendix),
    ].join("/");
  }

  static jarName(
    artifactId: string,
    version: string,
    classifier: string | undefined,
    appendix: string | undefined
  ): string {
    return (
      [artifactId, appendix, version, classifier]
        .filter((s) => s != undefined)
        .join("-") + ".jar"
    );
  }
}
