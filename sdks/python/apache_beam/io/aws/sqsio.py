#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import apache_beam as beam

from apache_beam.transforms.external import BeamJarExpansionService, SchemaAwareExternalTransform

__all__ = ['ReadFromSQS']

class ReadFromSQS(beam.PTransform):
    """Reads messages from an AWS SQS queue.
    
    Returns a PCollection of Beam Rows, each representing a SqsMessage object.
    This transform uses the Java SDK implementation, and most of AWS related connectivity configurations 
    can be set through a custom PipelineOptions object with the same properties seen on the AwsOptions 
    for Java SDK. For more information see: 
    https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/aws/sqs/SqsIO.html
    """
    URN = "beam:schematransform:org.apache.beam:aws:sqs_read:v1"
    
    def __init__(self, 
               queue_url,
               max_num_records=None,
               max_read_time_secs=None,
               expansion_service=None):
        """Initialize a ReadFromSQS transform.

        :param queue_url:
            The SQS queue url to use.
        :param max_num_records:
            The maximum number of records to read.
        :param max_read_time_secs:
            The maximum time to read from the queue.
        :param expansion_service:
            The address of the expansion service. If no expansion service is
            provided, will attempt to run the default AWS expansion service.
        """
        super().__init__()
        self._queue_url = queue_url
        self._max_num_records = max_num_records
        self._max_read_time_secs = max_read_time_secs
        self._expansion_service = (
            expansion_service or BeamJarExpansionService(
                'sdks:java:io:amazon-web-services2:expansion-service:build'))
        self.schematransform_config = SchemaAwareExternalTransform.discover_config(
            self._expansion_service, ReadFromSQS.URN)

    def expand(self, input):
        return (
            input.pipeline
            | "Use external transform" >> SchemaAwareExternalTransform(
                identifier=self.schematransform_config.identifier,
                expansion_service=self._expansion_service,
                rearrange_based_on_discovery=True,
                max_num_records=self._max_num_records,
                max_read_time_secs=self._max_read_time_secs,
                queue_url=self._queue_url))
        
