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
const findGitRoot = require("find-git-root");

// TODO: (Typescript) Why can't the var above be used as a namespace?
import { ChildProcess } from "child_process";
import { beamVersion } from "./packageJson";

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

class SubprocessServiceCache {
  parent?: SubprocessServiceCache;
  services: Map<string, SubprocessService> = new Map();

  constructor(parent: SubprocessServiceCache | undefined = undefined) {
    this.parent = parent;
  }

  put(key: string, service: SubprocessService) {
    this.services.set(key, service);
  }

  get(key: string): SubprocessService | undefined {
    if (this.services.has(key)) {
      return this.services.get(key);
    } else if (this.parent) {
      return this.parent?.get(key);
    } else {
      return undefined;
    }
  }

  async stopAll() {
    await Promise.all(
      [...this.services.values()].map((service) => {
        service.cached = false;
        return service.stop();
      }),
    );
  }
}

export class SubprocessService {
  static cache?: SubprocessServiceCache = undefined;

  process: ChildProcess;
  cmd: string;
  args: string[];
  name: string;
  address?: string;
  cached: boolean;

  constructor(
    cmd: string,
    args: string[],
    name: string | undefined = undefined,
  ) {
    this.cmd = cmd;
    this.args = args;
    this.name = name || cmd;
  }

  static async freePort(): Promise<number> {
    return new Promise((resolve) => {
      const srv = net.createServer();
      srv.listen(0, () => {
        const port = srv.address().port;
        srv.close((_) => resolve(port));
      });
    });
  }

  static createCache(): SubprocessServiceCache {
    SubprocessService.cache = new SubprocessServiceCache(
      SubprocessService.cache,
    );
    return this.cache!;
  }

  key(): string {
    return this.cmd + "\0" + this.args.join("\0");
  }

  async start(): Promise<string> {
    if (SubprocessService.cache) {
      const started = SubprocessService.cache.get(this.key());
      if (started) {
        this.cached = true;
        return started.address!;
      }
    }
    const host = "localhost";
    const port = (await SubprocessService.freePort()).toString();
    console.debug(
      this.cmd,
      this.args.map((arg) => arg.replace("{{PORT}}", port)),
    );
    this.process = childProcess.spawn(
      this.cmd,
      this.args.map((arg) => arg.replace("{{PORT}}", port)),
      {
        stdio: "inherit",
      },
    );

    try {
      console.debug(
        `Waiting for ${this.name} to be available on port ${port}.`,
      );
      await this.portReady(port, host, 10000);
      console.debug(`Service ${this.name} available.`);
    } catch (error) {
      this.process.kill();
      throw error;
    }

    this.address = host + ":" + port;

    if (SubprocessService.cache) {
      SubprocessService.cache.put(this.key(), this);
      this.cached = true;
    } else {
      this.cached = false;
    }
    return this.address!;
  }

  async stop() {
    if (this.cached) {
      return;
    }
    console.info(`Tearing down ${this.name}.`);
    this.address = undefined;
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
            connected = true;
            socket.end();
            resolve();
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
        "Timed out waiting for service after " + timeoutMs + "ms.",
      );
    }
  }
}

export function serviceProviderFromJavaGradleTarget(
  gradleTarget: string,
  args: string[] | undefined = undefined,
): () => Promise<Service> {
  return async () => {
    let jar: string;
    const serviceInfo = serviceOverrideFor(gradleTarget);
    if (serviceInfo) {
      if (serviceInfo.match(/^[a-zA-Z0-9.]+:[0-9]+$/)) {
        return new ExternalService(serviceInfo);
      } else {
        jar = serviceInfo;
      }
    } else {
      jar = await JavaJarService.cachedJar(
        await JavaJarService.gradleToJar(gradleTarget),
      );
    }

    return new JavaJarService(jar, args, gradleTarget);
  };
}

const BEAM_CACHE = path.join(os.homedir(), ".apache_beam", "cache");

export class JavaJarService extends SubprocessService {
  static APACHE_REPOSITORY = "https://repo.maven.apache.org/maven2";
  static BEAM_GROUP_ID = "org.apache.beam";
  static JAR_CACHE = path.join(BEAM_CACHE, "jars");

  constructor(
    jar: string,
    args: string[] | undefined = undefined,
    name: string | undefined = undefined,
  ) {
    if (!args) {
      // TODO: (Extension) Should filesToStage be set at some higher level?
      args = ["{{PORT}}", "--filesToStage=" + jar];
    }
    super("java", ["-jar", jar].concat(args), name);
  }

  static async cachedJar(
    urlOrPath: string,
    cacheDir: string = JavaJarService.JAR_CACHE,
  ): Promise<string> {
    if (urlOrPath.match(/^https?:\/\//)) {
      fs.mkdirSync(cacheDir, { recursive: true });
      const dest = path.join(
        JavaJarService.JAR_CACHE,
        path.basename(urlOrPath),
      );
      if (fs.existsSync(dest)) {
        return dest;
      }
      // TODO: (Cleanup) Use true temporary file.
      const tmp = dest + ".tmp" + Math.random();
      return new Promise((resolve, reject) => {
        const fout = fs.createWriteStream(tmp);
        console.warn("Downloading", urlOrPath);
        const request = https.get(urlOrPath, function (response) {
          if (response.statusCode !== 200) {
            reject(
              `Error code ${response.statusCode} when downloading ${urlOrPath}`,
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

  static async gradleToJar(
    gradleTarget: string,
    appendix: string | undefined = undefined,
    version: string = beamVersion,
  ): Promise<string> {
    if (version.startsWith("0.")) {
      // node-ts 0.x corresponds to Beam 2.x.
      version = "2" + version.substring(1);
    }
    const gradlePackage = gradleTarget.match(/^:?(.*):[^:]+:?$/)![1];
    const artifactId = "beam-" + gradlePackage.replaceAll(":", "-");
    const projectRoot = getBeamProjectRoot();
    const localPath = !projectRoot
      ? undefined
      : path.join(
          projectRoot,
          gradlePackage.replaceAll(":", path.sep),
          "build",
          "libs",
          JavaJarService.jarName(
            artifactId,
            version.replace("-SNAPSHOT", ""),
            "SNAPSHOT",
            appendix,
          ),
        );

    if (version.includes("SNAPSHOT") && !projectRoot) {
      version = "latest";
    }

    if (localPath && fs.existsSync(localPath)) {
      console.info("Using pre-built snapshot at", localPath);
      return localPath;
    } else if (version.includes("SNAPSHOT")) {
      throw new Error(
        `${localPath} not found. Please build the server with
      cd ${projectRoot}; ./gradlew ${gradleTarget})`,
      );
    } else {
      return JavaJarService.mavenJarUrl(
        artifactId,
        version,
        undefined,
        appendix,
      );
    }
  }

  static async mavenJarUrl(
    artifactId: string,
    version: string,
    classifier: string | undefined = undefined,
    appendix: string | undefined = undefined,
    repo: string = JavaJarService.APACHE_REPOSITORY,
    groupId: string = JavaJarService.BEAM_GROUP_ID,
  ): Promise<string> {
    if (version == "latest") {
      const medatadataUrl = [
        repo,
        groupId.replaceAll(".", "/"),
        artifactId,
        "maven-metadata.xml",
      ].join("/");
      const metadata = await new Promise<string>((resolve, reject) => {
        let data = "";
        https.get(medatadataUrl, (res) => {
          res.on("data", (chunk) => {
            data += chunk;
          });
          res.on("end", () => {
            resolve(data);
          });
          res.on("error", (e) => {
            reject(e);
          });
        });
      });
      version = metadata.match(/<latest>(.*)<\/latest>/)![1];
    }
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
    appendix: string | undefined,
  ): string {
    return (
      [artifactId, appendix, version, classifier]
        .filter((s) => s !== null && s !== undefined)
        .join("-") + ".jar"
    );
  }
}

export class PythonService extends SubprocessService {
  static VENV_CACHE = path.join(BEAM_CACHE, "venvs");

  static whichPython(): string {
    for (const bin of ["python3", "python"]) {
      try {
        const result = childProcess.spawnSync(bin, ["--version"]);
        if (result.status === 0) {
          return bin;
        }
      } catch (err) {
        // Try the next one.
      }
    }
    throw new Error("Can't find a Python executable.");
  }

  static beamPython(): string {
    const bootstrapScript = path.join(
      __dirname,
      "..",
      "..",
      "..",
      "resources",
      "bootstrap_beam_venv.py",
    );
    console.debug("Invoking Python bootstrap script.");
    const result = childProcess.spawnSync(
      PythonService.whichPython(),
      [bootstrapScript],
      { encoding: "latin1" },
    );
    if (result.status === 0) {
      console.debug(result.stdout);
      const lines = result.stdout.trim().split("\n");
      return lines[lines.length - 1];
    } else {
      throw new Error(result.output);
    }
  }

  static forModule(module: string, args: string[] = []): Service {
    let pythonExecutable: string;
    let serviceInfo = serviceOverrideFor("python:" + module);
    if (!serviceInfo) {
      serviceInfo = serviceOverrideFor("python:*");
    }
    if (serviceInfo) {
      if (serviceInfo.match(/^[a-zA-Z0-9.]+:[0-9]+$/)) {
        return new ExternalService(serviceInfo);
      } else {
        pythonExecutable = serviceInfo;
      }
    } else {
      pythonExecutable = PythonService.beamPython();
    }
    return new PythonService(pythonExecutable, module, args);
  }

  private constructor(
    pythonExecutablePath: string,
    module: string,
    args: string[] = [],
  ) {
    super(pythonExecutablePath, ["-u", "-m", module].concat(args), module);
  }
}

/**
 * Allows one to specify alternatives for auto-started services by specifying
 * an environment variable. For example,
 *
 * export BEAM_SERVICE_OVERRIDES='{
 *   "python:apache_beam.runners.portability.expansion_service_main":
 *       "localhost:port",
 *   "python:*": "/path/to/dev/venv/bin/python",
 *   "sdks:java:extensions:sql:expansion-service:shadowJar": "/path/to/jar"
 * }'
 *
 * would use the address localhost:port any time the Python service at
 * apache_beam.runners.portability.expansion_service_main was requested,
 * use the binary at /path/to/dev/venv/bin/python for starting up all other
 * services, and use /path/to/jar when the gradle target
 * sdks:java:extensions:sql:expansion-service:shadowJar was requested.
 *
 * This is primarily for testing.
 */
function serviceOverrideFor(name: string): string | undefined {
  if (process.env.BEAM_SERVICE_OVERRIDES) {
    return JSON.parse(process.env.BEAM_SERVICE_OVERRIDES)[name];
  }
}

function getBeamProjectRoot(): string | undefined {
  try {
    const projectRoot = path.dirname(findGitRoot(__dirname));
    if (
      fs.existsSync(
        path.join(projectRoot, "sdks", "typescript", "src", "apache_beam"),
      )
    ) {
      return projectRoot;
    } else {
      return undefined;
    }
  } catch (Error) {
    return undefined;
  }
}
