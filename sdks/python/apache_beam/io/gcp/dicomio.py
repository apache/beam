import apache_beam as beam
import google.auth
import json
from dicomclient import DicomApiHttpClient
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import PTransform
from apache_beam.io.filesystem import BeamIOError


class DicomSearch(PTransform):
  """A ``PTransform`` for QIDO search metadata from Cloud DICOM api.
    It takes Pcollection of dicts as input and return a Pcollection 
    of dict as results:
    INPUT:
    The input dict represents DICOM web path parameter, which hasfollowing 
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
      project_id: Id of the project in which DICOM store locates. (Required)
      region: Region where the DICOM store resides. (Required)
      dataset_id: Id of the dataset where DICOM store belongs to. (Required)
      dicom_store_id: Id of the dicom store. (Required)
      search_type: Which type of search it is, cloud only be one of the three 
        values: 'instances', 'series' or 'studies'. (Required)
      params: A dict of Tag:value pairs used to refine QIDO search. (Optional)
        Supported tags in three categories:
          1. Studies:
            StudyInstanceUID
            PatientName
            PatientID
            AccessionNumber
            ReferringPhysicianName
            StudyDate
          2. Series: all study level search terms and
            SeriesInstanceUID
            Modality
          3. Instances: all study/series level search terms and
            SOPInstanceUID
        e.g. {"StudyInstanceUID":"1","SeriesInstanceUID":"2","SOPInstanceUID":"3"}
    
    OUTPUT:
    The output dict encodes results as well as error messages:
    {
      'result': a list of dicts in JSON style
      'success': boolean value telling whether the operation is successful
      'input': detail ids and dicomweb path for this retrieval
      'status': status code from server, used as error message.
    }
  """

  def __init__(self, credential=None):
    """Initializes ``DicomSearch``.
    Args:
      credential: # type: Google credential object, if it isspecified, the 
        Http client will use it instead of the default one.
    """
    self.credential = credential

  def expand(self, pcoll):
    return pcoll | beam.ParDo(QidoSource(self.credential))


class QidoSource(beam.DoFn):
  """A DoFn execute every query input."""
  
  def __init__(self, credential=None):
    self.credential = credential
  
  def process(self, element):
    # Check if all required keys present.
    required_keys = [
      'project_id', 'region', 'dataset_id', 'dicom_store_id', 'search_type'
      ]
    
    error_message = None

    for key in required_keys:
      if key not in element:
        error_message = 'Must have %s in the dict.' % (key)
        break

    if not error_message:
      project_id = element['project_id']
      region = element['region']
      dataset_id = element['dataset_id']
      dicom_store_id = element['dicom_store_id']
      search_type = element['search_type']
      params = element['params'] if 'params' in element else None

      # call http client based on different types of search
      if element['search_type'] in ['instances', "studies", "series"]:
        result, status_code = DicomApiHttpClient().qido_search(
          project_id, region, dataset_id, dicom_store_id, 
          search_type, params, self.credential
        )
      else:
        error_message = 'Search type can only be "studies", "instances" or "series"'
      
      if not error_message:
        out = {}
        out['result'] = result
        out['status'] = status_code
        out['input'] = element
        out['success'] = True if status_code == 200 else False
        return [out]
    
    # when the input dict dose not meet the requirements. 
    out = {}
    out['result'] = []
    out['status'] = error_message
    out['input'] = element
    out['success'] = False
    return [out]

class PubsubToQido(PTransform):
  """A ``PTransform`` for converting pubsub messages into search input dict.
    Takes Pcollection of string as input and return a Pcollection of dict as 
    result. Note that some pubsub messages may not be from DICOM api, which
    will be records as failed conversions.
    INPUT:
    The input are normally strings from Pubsub topic:
      "projects/PROJECT_ID/locations/LOCATION/datasets/DATASET_ID/
      dicomStores/DICOM_STORE_ID/dicomWeb/studies/STUDY_UID/
      series/SERIES_UID/instances/INSTANCE_UID"
    OUTPUT:
    The output dict encodes results as well as error messages:
    {
      'result': a list of dicts in JSON style
      'success': boolean value telling whether the conversion is successful.
      'input': pubsub message string
    }
  """

  def __init__(self, credential=None): 
    # type: google credential object in google.auth
  
    """Initializes ``PubsubToQido``.
    Args:
      credential: # type: Google credential object, if it isspecified, the 
        Http client will use it instead of the default one.
    """
    self.credential = credential

  def expand(self, pcoll):
    return pcoll | beam.ParDo(ConvertPubsubToQido())


class ConvertPubsubToQido(beam.DoFn):
  """A DoFn execute every retrieval input."""
  def process(self, element):
    # Check if all required keys present.
    required_keys = [
      'projects', 'locations', 'datasets', 'dicomStores', 'dicomWeb',
      'studies', 'series', 'instances'
      ]
    
    entries = element.split('/')
    valid = True
    
    if len(entries) != 15:
      valid = False
    
    if valid:
      # check if the position of keys are correct
      for i in range(5):
        if required_keys[i] != entries[i*2]:
            valid = False
            break
      for i in range(5,8):
        if required_keys[i] != entries[i*2 - 1]:
            valid = False
            break
    
    if valid:
      # compose input dict for qido search
      qido_dict = {}
      qido_dict['project_id'] = entries[1]
      qido_dict['region'] = entries[3]
      qido_dict['dataset_id'] = entries[5]
      qido_dict['dicom_store_id'] = entries[7]
      qido_dict['search_type'] = 'instances'
      
      # compose instance level param for qido search
      params = {}
      params['StudyInstanceUID'] = entries[10]
      params['SeriesInstanceUID'] = entries[12]
      params['SOPInstanceUID'] = entries[14]
      qido_dict['params'] = params
      
      out = {}
      out['result'] = qido_dict
      out['input'] = element
      out['success'] = True

      return [out]
    else:
      # not a valid pubsub message from DICOM API
      out = {}
      out['result'] = {}
      out['input'] = element
      out['success'] = False
      return [out]


class DicomStoreInstance(PTransform):
  """A ``PTransform`` for storing instances to a DICOM store.
    Takes Pcollection of byte[] as input and return a Pcollection of dict as 
    result. The input are normally dicom file in bytes format.
    This sink needs to be initailzed by a destination dict:
    {
      'project_id': str,
      'region': str,
      'dataset_id': str,
      'dicom_store_id': str,
    }
    Key-value pairs:
      project_id: Id of the project in which DICOM store locates. (Required)
      region: Region where the DICOM store resides. (Required)
      dataset_id: Id of the dataset where DICOM store belongs to. (Required)
      dicom_store_id: Id of the dicom store. (Required)
    INPUT:
      Byte[] representing dicom file
    OUTPUT:
    The output dict encodes status as well as error messages:
    {
      'success': boolean value telling whether the store is successful
      'input': input file, only set if the trancation is failed.
      'status': status code from server, used as error message.
    }
    Todo: add stream object support for dcm file.
  """

  def __init__(self, destination_dict, credential=None): 
    """Initializes ``DicomStoreInstance``.
    Args:
      destination_dict: # type: python dict, more details in ConvertPubsubToQido.
      credential: # type: Google credential object, if it isspecified, the 
        Http client will use it instead of the default one.
    """
    self.credential = credential
    self.destination_dict = destination_dict

  def expand(self, pcoll):
    return pcoll | beam.ParDo(StoreInstanceBytes(self.destination_dict, self.credential))


class StoreInstanceBytes(beam.DoFn):
  """A DoFn execute every file input."""
  
  def __init__(self, destination_dict, credential=None):
    self.credential = credential
    required_keys = ['project_id', 'region', 'dataset_id', 'dicom_store_id']
    for key in required_keys:
      if key not in destination_dict:
        raise BeamIOError('Must have %s in the dict.' % (key))
    self.destination_dict = destination_dict
  
  def process(self, element):
    project_id = self.destination_dict['project_id']
    region = self.destination_dict['region']
    dataset_id = self.destination_dict['dataset_id']
    dicom_store_id = self.destination_dict['dicom_store_id']

    result, status_code = DicomApiHttpClient().dicomweb_store_instance(
      project_id, region, dataset_id, dicom_store_id, element, 
      self.credential
    )
    
    out = {}
    out['status'] = status_code
    out['input'] = None if status_code == 200 else element
    out['success'] = True if status_code == 200 else False
    return [out]
