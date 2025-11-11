import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.runners.worker.sdk_worker import get_ai_worker_pool_metadata

import grpc
import logging
import os
import sys


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'generated_proto')))

from envoy.service.ratelimit.v3 import rls_pb2
from envoy.service.ratelimit.v3 import rls_pb2_grpc
from envoy.extensions.common.ratelimit.v3 import ratelimit_pb2

# Set up logging
logging.basicConfig(level=logging.INFO)
_LOGGER = logging.getLogger(__name__)

class GRPCRateLimitClient(beam.DoFn):
    """
    A DoFn that makes gRPC calls to an Envoy Rate Limit Service.
    """
    def __init__(self):
        self._envoy_address = None
        self._channel = None
        self._stub = None

    def setup(self):
        """
        Initializes the gRPC channel and stub.
        """
        ai_worker_pool_metadata = get_ai_worker_pool_metadata()
        self._envoy_address = f"{ai_worker_pool_metadata.external_ip}:{ai_worker_pool_metadata.external_port}"
        _LOGGER.info(f"Setting up gRPC client for Envoy at {self._envoy_address}")
        self._channel = grpc.insecure_channel(self._envoy_address)
        self._stub = rls_pb2_grpc.RateLimitServiceStub(self._channel)

    def process(self, element):
        client_id = element.get('client_id', 'unknown_client')
        request_id = element.get('request_id', 'unknown_request')

        _LOGGER.info(f"Processing element: client_id={client_id}, request_id={request_id}")

        # Create a RateLimitDescriptor
        descriptor = ratelimit_pb2.RateLimitDescriptor()
        descriptor.entries.add(key="client_id", value=client_id)
        descriptor.entries.add(key="request_id", value=request_id)

        # Create a RateLimitRequest
        request = rls_pb2.RateLimitRequest(
            domain="my_service",
            descriptors=[descriptor],
            hits_addend=1
        )

        try:
            response = self._stub.ShouldRateLimit(request)
            _LOGGER.info(f"RateLimitResponse for client_id={client_id}, request_id={request_id}: {response.overall_code}")
            yield {
                'client_id': client_id,
                'request_id': request_id,
                'rate_limit_status': rls_pb2.RateLimitResponse.Code.Name(response.overall_code),
                'response_details': str(response)
            }
        except grpc.RpcError as e:
            _LOGGER.error(f"gRPC call failed for client_id={client_id}, request_id={request_id}: {e.details()}")
            yield {
                'client_id': client_id,
                'request_id': request_id,
                'rate_limit_status': 'ERROR',
                'error_details': e.details()
            }

    def teardown(self):
        if self._channel:
            _LOGGER.info("Tearing down gRPC client.")
            self._channel.close()

def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).runner = 'DirectRunner' # Use DirectRunner for local testing

    with beam.Pipeline(options=options) as p:
        # Sample input data
        requests = p | 'CreateRequests' >> beam.Create([
            {'client_id': 'user_1', 'request_id': 'req_a'},
            {'client_id': 'user_2', 'request_id': 'req_b'},
            {'client_id': 'user_1', 'request_id': 'req_c'},
            {'client_id': 'user_3', 'request_id': 'req_d'},
        ])

        # Apply the gRPC client DoFn
        rate_limit_results = requests | 'CheckRateLimit' >> beam.ParDo(GRPCRateLimitClient())

        # Log the results
        rate_limit_results | 'LogResults' >> beam.Map(lambda x: _LOGGER.info(f"Result: {x}"))

if __name__ == '__main__':
    run()
