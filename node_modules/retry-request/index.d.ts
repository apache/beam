declare module 'retry-request' {
  // eslint-disable-next-line node/no-unpublished-import
  import * as request from 'request';

  namespace retryRequest {
    function getNextRetryDelay(retryNumber: number): void;
    interface Options {
      objectMode?: boolean;
      request?: typeof request;
      retries?: number;
      noResponseRetries?: number;
      currentRetryAttempt?: number;
      maxRetryDelay?: number;
      retryDelayMultiplier?: number;
      totalTimeout?: number;
      shouldRetryFn?: (response: request.RequestResponse) => boolean;
    }
  }

  function retryRequest(
    requestOpts: request.Options,
    opts: retryRequest.Options,
    callback?: request.RequestCallback
  ): {abort: () => void};
  function retryRequest(
    requestOpts: request.Options,
    callback?: request.RequestCallback
  ): {abort: () => void};

  export = retryRequest;
}
