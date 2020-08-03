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

"""DICOM IO connector
This module implements several tools to facilitate the interaction between
a Google Cloud Healthcare DICOM store and a Beam pipeline.

For more details on DICOM store and API:
https://cloud.google.com/healthcare/docs/how-tos/dicom
The DICOM IO connector can be used to search metadata or write DICOM files
to DICOM store. When used together with Google Pubsub message connector,
a PTransform implemented in this module can be used to convert pubsub
messages to search requests. Since Traceability is crucial for healthcare
API users, every input or error message will be recorded in the output of
the DICOM IO connector. As a result, every PTransform in this module will
return a PCollection of dict that encodes results and detailed error messages.

Search instance's metadata (QIDO request)
===================================================
DicomSearch() wraps the QIDO request client and supports 3 levels of search.
Users should specify the level by setting the 'search_type' entry in the input
dict. They can also refine the search by adding tags to filter the results using
the 'params' entry. Here is a sample usage:

  with Pipeline() as p:
    input_dict = p | beam.Create(
      [{'project_id': 'abc123', 'type': 'instances',...},
      {'project_id': 'dicom_go', 'type': 'series',...}])

    results = input_dict | io.gcp.DicomSearch()
    results | 'print successful search' >> beam.Map(
    lambda x: print(x['result'] if x['success'] else None))

    results | 'print failed search' >> beam.Map(
    lambda x: print(x['result'] if not x['success'] else None))

In the example above, successful qido search results and error messages for
failed requests are printed. When used in real life, user can choose to filter
those data and output them to wherever they want.

Convert DICOM Pubsub message to Qido search request
===================================================
Healthcare API users might use Beam's Pubsub streaming pipeline to monitor the
store operations (new DICOM file) in a DICOM storage. Pubsub message encodes
DICOM a web store path as well as instance ids. If users are interested in
getting new instance's metadata, they can use PubsubToQido() to convert the
message into Qido Search dict then use DicomSearch(). Here is a sample usage:

  pipeline_options = PipelineOptions()
  pipeline_options.view_as(StandardOptions).streaming = True
  p =  beam.Pipeline(options=pipeline_options)
  pubsub = p | beam.io.ReadStringFromPubsub(subscription='a_dicom_store')
  results = pubsub | PubsubToQido()
  success = results | 'filter message' >> beam.Filter(lambda x: x['success'])
  qido_dict = success | 'get qido request' >> beam.Map(lambda x: x['result'])
  metadata = qido_dict | DicomSearch()

In the example above, the pipeline is listening to a pubsub topic and waiting
for messages from DICOM API. When a new DICOM file comes into the storage, the
pipeline will receive a pubsub message, convert it to a Qido request dict and
feed it to DicomSearch() PTransform. As a result, users can get the metadata for
every new DICOM file. Note that not every pubsub message received is from DICOM
API, so we to filter the results first.

Store a DICOM file in a DICOM storage
===================================================
DicomStoreInstance() wraps store request API and users can use it to send a
DICOM file to a DICOM store. It supports two types of input: 1.file data in
byte[] 2.fileio object. Users should set the 'input_type' when initialzing
this PTransform. Here are the examples:

  with Pipeline() as p:
    input_dict = {'project_id': 'abc123', 'type': 'instances',...}
    path = "gcs://bucketname/something/a.dcm"
    match = p | fileio.MatchFiles(path)
    fileio_obj = match | fileio.ReadAll()
    results = fileio_obj | DicomStoreInstance(input_dict, 'fileio')

  with Pipeline() as p:
    input_dict = {'project_id': 'abc123', 'type': 'instances',...}
    f = open("abc.dcm", "rb")
    dcm_file = f.read()
    byte_file = p | 'create byte file' >> beam.Create([dcm_file])
    results = byte_file | DicomStoreInstance(input_dict, 'bytes')

The first example uses a PCollection of fileio objects as input.
DicomStoreInstance will read DICOM files from the objects and send them
to a DICOM storage.
The second example uses a PCollection of byte[] as input. DicomStoreInstance
will directly send those DICOM files to a DICOM storage.
Users can also get the operation results in the output PCollection if they want
to handle the failed store requests.
"""

# pytype: skip-file
from __future__ import absolute_import

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

import apache_beam as beam
from apache_beam.io.gcp.dicomclient import DicomApiHttpClient
from apache_beam.transforms import PTransform


class DicomSearch(PTransform):
  """A PTransform used for retrieving DICOM instance metadata from Google
    Cloud DICOM store. It takes a PCollection of dicts as input and return
    a PCollection of dict as results:
    INPUT:
    The input dict represents DICOM web path parameters, which has the following
    string keys and values:
    {
    'project_id': str,
    'region': str,
    'dataset_id': str,
    'dicom_store_id': str,
    'search_type': str,
    'params': dict(str,str) (Optional),
    }

    Key-value pairs:
      project_id: Id of the project in which the DICOM store is
      located. (Required)
      region: Region where the DICOM store resides. (Required)
      dataset_id: Id of the dataset where DICOM store belongs to. (Required)
      dicom_store_id: Id of the dicom store. (Required)
      search_type: Which type of search it is, could only be one of the three
      values: 'instances', 'series', or 'studies'. (Required)
      params: A dict of str:str pairs used to refine QIDO search. (Optional)
      Supported tags in three categories:
      1.Studies:
      * StudyInstanceUID,
      * PatientName,
      * PatientID,
      * AccessionNumber,
      * ReferringPhysicianName,
      * StudyDate,
      2.Series: all study level search terms and
      * SeriesInstanceUID,
      * Modality,
      3.Instances: all study/series level search terms and
      * SOPInstanceUID,

      e.g. {"StudyInstanceUID":"1","SeriesInstanceUID":"2"}

    OUTPUT:
    The output dict wraps results as well as error messages:
    {
    'result': a list of dicts in JSON style.
    'success': boolean value telling whether the operation is successful.
    'input': detail ids and dicomweb path for this retrieval.
    'status': status code from the server, used as error message.
    }

  """
  def __init__(
      self, buffer_size=8, max_workers=5, client=None, credential=None):
    """Initializes DicomSearch.
    Args:
      buffer_size: # type: Int. Size of the request buffer.
      max_workers: # type: Int. Maximum number of threads a worker can
      create. If it is set to one, all the request will be processed
      sequentially in a worker.
      client: # type: object. If it is specified, all the Api calls will
      made by this client instead of the default one (DicomApiHttpClient).
      credential: # type: Google credential object, if it is specified, the
      Http client will use it to create sessions instead of the default.
    """
    self.buffer_size = buffer_size
    self.max_workers = max_workers
    self.client = client or DicomApiHttpClient()
    self.credential = credential

  def expand(self, pcoll):
    return pcoll | beam.ParDo(
        _QuidoReadFn(
            self.buffer_size, self.max_workers, self.client, self.credential))


class _QuidoReadFn(beam.DoFn):
  """A DoFn for executing every qido query request."""
  def __init__(self, buffer_size, max_workers, client, credential=None):
    self.buffer_size = buffer_size
    self.max_workers = max_workers
    self.client = client
    self.credential = credential

  def start_bundle(self):
    self.buffer = []

  def finish_bundle(self):
    for item in self._flush():
      yield item

  def validate_element(self, element):
    # Check if all required keys present.
    required_keys = [
        'project_id', 'region', 'dataset_id', 'dicom_store_id', 'search_type'
    ]

    for key in required_keys:
      if key not in element:
        error_message = 'Must have %s in the dict.' % (key)
        return False, error_message

    # Check if return type is correct.
    if element['search_type'] in ['instances', "studies", "series"]:
      return True, None
    else:
      error_message = (
          'Search type can only be "studies", '
          '"instances" or "series"')
      return False, error_message

  def process(
      self,
      element,
      window=beam.DoFn.WindowParam,
      timestamp=beam.DoFn.TimestampParam):
    # Check if the element is valid
    valid, error_message = self.validate_element(element)

    if valid:
      self.buffer.append((element, window, timestamp))
      if len(self.buffer) >= self.buffer_size:
        for item in self._flush():
          yield item
    else:
      # Return this when the input dict dose not meet the requirements
      out = {}
      out['result'] = []
      out['status'] = error_message
      out['input'] = element
      out['success'] = False
      yield out

  def make_request(self, element):
    # Sending Qido request to DICOM Api
    project_id = element['project_id']
    region = element['region']
    dataset_id = element['dataset_id']
    dicom_store_id = element['dicom_store_id']
    search_type = element['search_type']
    params = element['params'] if 'params' in element else None

    # Call qido search http client
    result, status_code = self.client.qido_search(
      project_id, region, dataset_id, dicom_store_id,
      search_type, params, self.credential
    )

    out = {}
    out['result'] = result
    out['status'] = status_code
    out['input'] = element
    out['success'] = (status_code == 200)
    return out

  def process_buffer_element(self, buffer_element):
    # Thread job runner - each thread makes a Qido search request
    value = self.make_request(buffer_element[0])
    windows = [buffer_element[1]]
    timestamp = buffer_element[2]
    return beam.utils.windowed_value.WindowedValue(
        value=value, timestamp=timestamp, windows=windows)

  def _flush(self):
    # Create thread pool executor and process the buffered elements in paralllel
    executor = ThreadPoolExecutor(max_workers=self.max_workers)
    futures = [
        executor.submit(self.process_buffer_element, ele) for ele in self.buffer
    ]
    self.buffer = []
    for f in as_completed(futures):
      yield f.result()


class PubsubToQido(PTransform):
  """A PTransform for converting pubsub messages into search input dict.
    Takes PCollection of string as input and returns a PCollection of dict as
    results. Note that some pubsub messages may not be from DICOM API, which
    will be recorded as failed conversions.
    INPUT:
    The input are normally strings from Pubsub topic:
    "projects/PROJECT_ID/locations/LOCATION/datasets/DATASET_ID/
    dicomStores/DICOM_STORE_ID/dicomWeb/studies/STUDY_UID/
    series/SERIES_UID/instances/INSTANCE_UID"

    OUTPUT:
    The output dict encodes results as well as error messages:
    {
    'result': a dict representing instance level qido search request.
    'success': boolean value telling whether the conversion is successful.
    'input': input pubsub message string.
    }

  """
  def __init__(self, credential=None):
    """Initializes PubsubToQido.
    Args:
      credential: # type: Google credential object, if it is specified, the
      Http client will use it instead of the default one.
    """
    self.credential = credential

  def expand(self, pcoll):
    return pcoll | beam.ParDo(_ConvertPubsubToQido())


class _ConvertPubsubToQido(beam.DoFn):
  """A DoFn for converting pubsub string to qido search parameters."""
  def process(self, element):
    # Some constants for DICOM pubsub message
    NUM_PUBSUB_STR_ENTRIES = 15
    NUM_DICOM_WEBPATH_PARAMETERS = 5
    NUM_TOTAL_PARAMETERS = 8
    INDEX_PROJECT_ID = 1
    INDEX_REGION = 3
    INDEX_DATASET_ID = 5
    INDEX_DICOMSTORE_ID = 7
    INDEX_STUDY_ID = 10
    INDEX_SERIE_ID = 12
    INDEX_INSTANCE_ID = 14

    entries = element.split('/')

    # Output dict with error message, used when
    # receiving invalid pubsub string.
    error_dict = {}
    error_dict['result'] = {}
    error_dict['input'] = element
    error_dict['success'] = False

    if len(entries) != NUM_PUBSUB_STR_ENTRIES:
      return [error_dict]

    required_keys = [
        'projects',
        'locations',
        'datasets',
        'dicomStores',
        'dicomWeb',
        'studies',
        'series',
        'instances'
    ]

    # Check if the required keys present and
    # the positions of those keys are correct
    for i in range(NUM_DICOM_WEBPATH_PARAMETERS):
      if required_keys[i] != entries[i * 2]:
        return [error_dict]
    for i in range(NUM_DICOM_WEBPATH_PARAMETERS, NUM_TOTAL_PARAMETERS):
      if required_keys[i] != entries[i * 2 - 1]:
        return [error_dict]

    # Compose dicom webpath parameters for qido search
    qido_dict = {}
    qido_dict['project_id'] = entries[INDEX_PROJECT_ID]
    qido_dict['region'] = entries[INDEX_REGION]
    qido_dict['dataset_id'] = entries[INDEX_DATASET_ID]
    qido_dict['dicom_store_id'] = entries[INDEX_DICOMSTORE_ID]
    qido_dict['search_type'] = 'instances'

    # Compose instance level params for qido search
    params = {}
    params['StudyInstanceUID'] = entries[INDEX_STUDY_ID]
    params['SeriesInstanceUID'] = entries[INDEX_SERIE_ID]
    params['SOPInstanceUID'] = entries[INDEX_INSTANCE_ID]
    qido_dict['params'] = params

    out = {}
    out['result'] = qido_dict
    out['input'] = element
    out['success'] = True

    return [out]


class WriteToDicomStore(PTransform):
  """A PTransform for storing instances to a DICOM store.
    Takes PCollection of byte[] as input and return a PCollection of dict as
    results. The inputs are normally DICOM file in bytes or str filename.
    INPUT:
    This PTransform supports two types of input:
    1. Byte[]: representing dicom file.
    2. Fileio object: stream file object.

    OUTPUT:
    The output dict encodes status as well as error messages:
    {
    'success': boolean value telling whether the store is successful.
    'input': undeliverable data. Exactly the same as the input,
    only set if the operation is failed.
    'status': status code from the server, used as error messages.
    }

  """
  def __init__(
      self,
      destination_dict,
      input_type,
      buffer_size=8,
      max_workers=5,
      client=None,
      credential=None):
    """Initializes WriteToDicomStore.
    Args:
      destination_dict: # type: python dict, encodes DICOM endpoint information:
      {
      'project_id': str,
      'region': str,
      'dataset_id': str,
      'dicom_store_id': str,
      }

      Key-value pairs:
      * project_id: Id of the project in which DICOM store locates. (Required)
      * region: Region where the DICOM store resides. (Required)
      * dataset_id: Id of the dataset where DICOM store belongs to. (Required)
      * dicom_store_id: Id of the dicom store. (Required)

      input_type: # type: string, could only be 'bytes' or 'fileio'
      buffer_size: # type: Int. Size of the request buffer.
      max_workers: # type: Int. Maximum number of threads a worker can
      create. If it is set to one, all the request will be processed
      sequentially in a worker.
      client: # type: object. If it is specified, all the Api calls will
      made by this client instead of the default one (DicomApiHttpClient).
      credential: # type: Google credential object, if it is specified, the
      Http client will use it instead of the default one.
    """
    self.destination_dict = destination_dict
    # input_type pre-check
    if input_type not in ['bytes', 'fileio']:
      raise ValueError("input_type could only be 'bytes' or 'fileio'")
    self.input_type = input_type
    self.buffer_size = buffer_size
    self.max_workers = max_workers
    self.client = client
    self.credential = credential

  def expand(self, pcoll):
    return pcoll | beam.ParDo(
        _StoreInstance(
            self.destination_dict,
            self.input_type,
            self.buffer_size,
            self.max_workers,
            self.client,
            self.credential))


class _StoreInstance(beam.DoFn):
  """A DoFn read or fetch dicom files then push it to a dicom store."""
  def __init__(
      self,
      destination_dict,
      input_type,
      buffer_size,
      max_workers,
      client,
      credential=None):
    # pre-check destination dict
    required_keys = ['project_id', 'region', 'dataset_id', 'dicom_store_id']
    for key in required_keys:
      if key not in destination_dict:
        raise ValueError('Must have %s in the dict.' % (key))
    self.destination_dict = destination_dict
    self.input_type = input_type
    self.buffer_size = buffer_size
    self.max_workers = max_workers
    self.client = client
    self.credential = credential

  def start_bundle(self):
    self.buffer = []

  def finish_bundle(self):
    for item in self._flush():
      yield item

  def process(
      self,
      element,
      window=beam.DoFn.WindowParam,
      timestamp=beam.DoFn.TimestampParam):
    self.buffer.append((element, window, timestamp))
    if len(self.buffer) >= self.buffer_size:
      for item in self._flush():
        yield item

  def make_request(self, dicom_file):
    # Send file to DICOM store and records the results.
    project_id = self.destination_dict['project_id']
    region = self.destination_dict['region']
    dataset_id = self.destination_dict['dataset_id']
    dicom_store_id = self.destination_dict['dicom_store_id']

    # Feed the dicom file into store client
    if self.client:
      _, status_code = self.client.dicomweb_store_instance(
        project_id, region, dataset_id, dicom_store_id, dicom_file,
        self.credential
      )
    else:
      _, status_code = DicomApiHttpClient().dicomweb_store_instance(
        project_id, region, dataset_id, dicom_store_id, dicom_file,
        self.credential
      )

    out = {}
    out['status'] = status_code
    out['success'] = (status_code == 200)
    return out

  def read_dicom_file(self, buffer_element):
    # Read the file based on different input. If the read fails ,return
    # an error dict which records input and error messages.
    try:
      if self.input_type == 'fileio':
        f = buffer_element.open()
        data = f.read()
        f.close()
        return True, data
      else:
        return True, buffer_element
    except Exception as error_message:
      error_out = {}
      error_out['status'] = error_message
      error_out['success'] = False
      return False, error_out

  def process_buffer_element(self, buffer_element):
    # Thread job runner - each thread stores a DICOM file
    success, read_result = self.read_dicom_file(buffer_element[0])
    windows = [buffer_element[1]]
    timestamp = buffer_element[2]
    value = None
    if success:
      value = self.make_request(read_result)
    else:
      value = read_result
    # save the undeliverable data
    if not value['success']:
      value['input'] = buffer_element[0]
    return beam.utils.windowed_value.WindowedValue(
        value=value, timestamp=timestamp, windows=windows)

  def _flush(self):
    # Create thread pool executor and process the buffered elements in paralllel
    executor = ThreadPoolExecutor(max_workers=self.max_workers)
    futures = [
        executor.submit(self.process_buffer_element, ele) for ele in self.buffer
    ]
    self.buffer = []
    for f in as_completed(futures):
      yield f.result()
