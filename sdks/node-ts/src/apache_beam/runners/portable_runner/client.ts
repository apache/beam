import { ChannelCredentials } from "@grpc/grpc-js";
import { GrpcTransport } from "@protobuf-ts/grpc-transport";
import { RpcTransport } from "@protobuf-ts/runtime-rpc";
import { Struct } from "../../proto/google/protobuf/struct";
import { PrepareJobRequest } from "../../proto/beam_job_api";
import {
  JobServiceClient,
  IJobServiceClient,
} from "../../proto/beam_job_api.client";

import * as runnerApiProto from "../../proto/beam_runner_api";

/**
 * Wrapper for JobServiceClient.
 */
export class RemoteJobServiceClient {
  client: JobServiceClient;

  /**
   * @param host Host and port of JobService server.
   * @param {optional} transport By default, GrpcTransport is used. Supply an RpcTransport to override.
   * @param channelCredentials ChannelCredentials.createInsecure() by default. Override with a ChannelCredentials.
   */
  constructor(
    host: string,
    transport?: RpcTransport,
    channelCredentials?: ChannelCredentials
  ) {
    transport =
      transport ||
      new GrpcTransport({
        host,
        channelCredentials:
          channelCredentials || ChannelCredentials.createInsecure(),
      });
    this.client = new JobServiceClient(transport!);
  }

  async prepare(
    pipeline: runnerApiProto.Pipeline,
    jobName: string,
    pipelineOptions?: Struct
  ) {
    let message: PrepareJobRequest = { pipeline, jobName };
    if (pipelineOptions) {
      message.pipelineOptions = pipelineOptions;
    }
    const call = this.client.prepare(message);
    return await call.response;
  }

  async run(preparationId: string) {
    const call = this.client.run({ preparationId, retrievalToken: "" });
    return await call.response;
  }

  async getState(jobId: string) {
    const call = this.client.getState({ jobId });
    return await call.response;
  }
}
