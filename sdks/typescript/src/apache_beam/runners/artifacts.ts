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

var crypto = require("crypto");
var fs = require("fs");
var path = require("path");
var os = require("os");

import * as runnerApi from "../proto/beam_runner_api";
import * as artifactApi from "../proto/beam_artifact_api";

import {
  IArtifactRetrievalServiceClient,
  IArtifactStagingServiceClient,
} from "../proto/beam_artifact_api.client";

const defaultArtifactDir = path.join(
  os.homedir(),
  ".apache_beam",
  "cache",
  "artifacts",
);

/**
 * Downloads the required artifacts from the service to the destination
 * directory.
 */
export async function* resolveArtifacts(
  client: IArtifactRetrievalServiceClient,
  artifacts: Iterable<runnerApi.ArtifactInformation>,
  localDir: string = defaultArtifactDir,
): AsyncGenerator<runnerApi.ArtifactInformation, void, unknown> {
  const resolved = await client.resolveArtifacts({
    artifacts: Array.from(artifacts),
    preferredUrns: [],
  }).response;

  async function storeArtifact(
    artifact: runnerApi.ArtifactInformation,
  ): Promise<runnerApi.ArtifactInformation> {
    if (artifact.typeUrn === "beam:artifact:type:file:v1") {
      const payload = runnerApi.ArtifactFilePayload.fromBinary(
        artifact.typePayload,
      );
      // As we're storing artifacts by hash, we can safely re-use if we've
      // ever seen this before.
      if (
        fs.existsSync(payload.sha256 && path.join(localDir, payload.sha256))
      ) {
        return {
          typeUrn: "beam:artifact:type:file:v1",
          typePayload: runnerApi.ArtifactFilePayload.toBinary({
            path: path.join(localDir, payload.sha256),
            sha256: payload.sha256,
          }),
          roleUrn: artifact.roleUrn,
          rolePayload: artifact.rolePayload,
        };
      }
      // TODO: (Perf) Hardlink if same filesystem and correct hash?
    }

    fs.mkdirSync(localDir, { recursive: true });
    const tmpName = tempFile(localDir);
    const tmpHandle = fs.createWriteStream(tmpName);
    const call = client.getArtifact({ artifact: artifact });
    const hasher = crypto.createHash("sha256");
    call.responses.onMessage((msg) => {
      hasher.update(msg.data);
      tmpHandle.write(msg.data);
    });
    await call;
    tmpHandle.close();
    const hash = hasher.digest("hex");
    const finalPath = path.join(localDir, hash);
    fs.renameSync(tmpName, finalPath);
    return {
      typeUrn: "beam:artifact:type:file:v1",
      typePayload: runnerApi.ArtifactFilePayload.toBinary({
        path: finalPath,
        sha256: hash,
      }),
      roleUrn: artifact.roleUrn,
      rolePayload: artifact.rolePayload,
    };
  }

  for (const artifact of resolved.replacements) {
    if (
      artifact.typeUrn === "beam:artifact:type:url:v1" ||
      artifact.typeUrn === "beam:artifact:type:embedded:v1"
    ) {
      // TODO: (Typescript) Yield from asycn?
      yield artifact;
    } else {
      yield await storeArtifact(artifact);
    }
  }
}

export async function offerArtifacts(
  client: IArtifactStagingServiceClient,
  stagingToken: string,
  rootDir: string = defaultArtifactDir,
) {
  const call = client.reverseArtifactRetrievalService();
  call.responses.onMessage(async (msg) => {
    switch (msg.request.oneofKind) {
      case "resolveArtifact":
        call.requests.send({
          stagingToken: stagingToken,
          isLast: false,
          response: {
            oneofKind: "resolveArtifactResponse",
            resolveArtifactResponse: {
              replacements: msg.request.resolveArtifact.artifacts,
            },
          },
        });
        break;

      case "getArtifact":
        switch (msg.request.getArtifact.artifact!.typeUrn) {
          case "beam:artifact:type:file:v1":
            const payload = runnerApi.ArtifactFilePayload.fromBinary(
              msg.request.getArtifact.artifact!.typePayload,
            );
            const filePath = path.normalize(payload.path);
            if (!filePath.startsWith(rootDir)) {
              throw new Error(
                "Refusing to serve " +
                  filePath +
                  " as it is not under " +
                  rootDir,
              );
            }
            const handle = fs.createReadStream(filePath);
            for await (const chunk of handle) {
              call.requests.send({
                stagingToken: stagingToken,
                isLast: false,
                response: {
                  oneofKind: "getArtifactResponse",
                  getArtifactResponse: { data: chunk },
                },
              });
            }
            call.requests.send({
              stagingToken: stagingToken,
              isLast: true,
              response: {
                oneofKind: "getArtifactResponse",
                getArtifactResponse: { data: new Uint8Array() },
              },
            });
            break;

          default:
            throw new Error(
              "Unknown artifact type " +
                msg.request.getArtifact.artifact!.typeUrn,
            );
        }
        break;

      default:
        throw new Error("Unknown request type " + msg.request.oneofKind);
    }
  });

  call.requests.send({
    stagingToken: stagingToken,
    isLast: false,
    response: {
      oneofKind: undefined,
    },
  });
  await call;
}

function tempFile(dir) {
  return path.join(
    dir,
    "temp" +
      crypto
        .randomBytes(16)
        .toString("base64")
        .replace(/[^a-zA-Z0-9]/g, "_"),
  );
}
