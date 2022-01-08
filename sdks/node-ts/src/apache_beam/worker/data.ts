import * as grpc from '@grpc/grpc-js';

import {Elements} from '../proto/beam_fn_api';
import {
  ProcessBundleDescriptor,
  ProcessBundleResponse,
} from '../proto/beam_fn_api';
import {
  BeamFnDataClient,
  IBeamFnDataClient,
} from '../proto/beam_fn_api.grpc-client';

export class MultiplexingDataChannel {
  dataClient: BeamFnDataClient;
  dataChannel: grpc.ClientDuplexStream<Elements, Elements>;

  consumers: Map<string, Map<string, IDataChannel>> = new Map();

  constructor(endpoint: string, workerId: string) {
    const metadata = new grpc.Metadata();
    metadata.add('worker_id', workerId);
    this.dataClient = new BeamFnDataClient(
      endpoint,
      grpc.ChannelCredentials.createInsecure(),
      {},
      {}
    );
    this.dataChannel = this.dataClient.data(metadata);
    this.dataChannel.on('data', elements => {
      console.log('data', elements);
      for (const data of elements.data) {
        const consumer = this.getConsumer(data.instructionId, data.transformId);
        try {
          consumer.sendData(data.data);
          if (data.is_last) {
            consumer.close();
          }
        } catch (error) {
          consumer.onError(error);
        }
      }
      for (const timers of elements.timers) {
        const consumer = this.getConsumer(
          timers.instructionId,
          timers.transformId
        );
        try {
          consumer.sendTimers(timers.timerFamilyId, timers.timers);
          if (timers.is_last) {
            consumer.close();
          }
        } catch (error) {
          consumer.onError(error);
        }
      }
    });
  }

  registerConsumer(
    bundleId: string,
    transformId: string,
    consumer: IDataChannel
  ) {
    consumer = new TruncateOnErrorDataChannel(consumer);
    if (!this.consumers.has(bundleId)) {
      this.consumers.set(bundleId, new Map());
    }
    if (this.consumers.get(bundleId)!.has(transformId)) {
      (
        this.consumers.get(bundleId)!.get(transformId) as BufferingDataChannel
      ).flush(consumer);
    }
    this.consumers.get(bundleId)!.set(transformId, consumer);
  }

  unregisterConsumer(bundleId: string, transformId: string) {
    this.consumers.get(bundleId)!.delete(transformId);
  }

  getConsumer(bundleId: string, transformId: string): IDataChannel {
    if (!this.consumers.has(bundleId)) {
      this.consumers.set(bundleId, new Map());
    }
    if (!this.consumers.get(bundleId)!.has(transformId)) {
      this.consumers
        .get(bundleId)!
        .set(transformId, new BufferingDataChannel());
    }
    return this.consumers.get(bundleId)!.get(transformId)!;
  }

  getSendChannel(bundleId: string, transformId: string): IDataChannel {
    // TODO: Buffer and consilidate send requests?
    const this_ = this;
    return {
      sendData: function (data: Uint8Array) {
        this_.dataChannel.write({
          data: [
            {
              instructionId: bundleId,
              transformId: transformId,
              data: data,
              isLast: false,
            },
          ],
          timers: [],
        });
      },
      sendTimers: function (timerFamilyId: string, timers: Uint8Array) {
        throw Error('Timers not yet supported.');
      },
      close: function () {
        this_.dataChannel.write({
          data: [
            {
              instructionId: bundleId,
              transformId: transformId,
              data: new Uint8Array(),
              isLast: true,
            },
          ],
          timers: [],
        });
      },
      onError: function (error: Error) {
        throw error;
      },
    };
  }
}

export interface IDataChannel {
  // TODO: onData?
  sendData: (data: Uint8Array) => void;
  sendTimers: (timerFamilyId: string, timers: Uint8Array) => void;
  close: () => void;
  onError: (Error) => void;
}

class BufferingDataChannel implements IDataChannel {
  data: Uint8Array[] = [];
  timers: [string, Uint8Array][] = [];
  closed = false;
  error?: Error;

  sendData(data: Uint8Array) {
    this.data.push(data);
  }

  sendTimers(timerFamilyId: string, timers: Uint8Array) {
    this.timers.push([timerFamilyId, timers]);
  }

  close() {
    this.closed = true;
  }

  onError(error: Error) {
    this.closed = true;
    this.error = error;
  }

  flush(channel: IDataChannel) {
    this.data.forEach(channel.sendData.bind(channel));
    this.timers.forEach(([timerFamilyId, timers]) =>
      channel.sendTimers(timerFamilyId, timers)
    );
    if (this.error) {
      channel.onError(this.error);
    }
    if (this.closed) {
      channel.close();
    }
  }
}

class TruncateOnErrorDataChannel implements IDataChannel {
  private seenError = false;

  constructor(private underlying: IDataChannel) {}

  sendData(data: Uint8Array) {
    this.underlying.sendData(data);
  }

  sendTimers(timerFamilyId: string, timers: Uint8Array) {
    this.underlying.sendTimers(timerFamilyId, timers);
  }

  close() {
    this.underlying.close();
  }

  onError(error: Error) {
    console.error('DATA ERROR', error);
    this.seenError = true;
    this.underlying.onError(error);
  }
}
