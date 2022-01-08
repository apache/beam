import {ChannelCredentials} from '@grpc/grpc-js';
import {GrpcTransport} from '@protobuf-ts/grpc-transport';
import {RpcTransport} from '@protobuf-ts/runtime-rpc';
import {Struct} from '../../proto/google/protobuf/struct';
import {PrepareJobRequest} from '../../proto/beam_job_api';
import {
  JobServiceClient,
  IJobServiceClient,
} from '../../proto/beam_job_api.client';

import * as runnerApiProto from '../../proto/beam_runner_api';

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
    return await this.callPrepare(
      this.client,
      pipeline,
      jobName,
      pipelineOptions
    );
  }

  async run(preparationId: string) {
    return await this.callRun(this.client, preparationId);
  }

  async getState(jobId: string) {
    return await this.callGetState(this.client, jobId);
  }

  private async callPrepare(
    client: IJobServiceClient,
    pipeline: runnerApiProto.Pipeline,
    jobName: string,
    pipelineOptions?: Struct
  ) {
    const message: PrepareJobRequest = {pipeline, jobName};
    if (pipelineOptions) {
      message.pipelineOptions = pipelineOptions;
    }
    const call = client.prepare(message);
    return await call.response;
  }

  private async callRun(client: IJobServiceClient, preparationId: string) {
    const call = client.run({preparationId, retrievalToken: ''});
    return await call.response;
  }

  private async callGetState(client: IJobServiceClient, jobId: string) {
    const call = client.getState({jobId});
    return await call.response;
  }
}
