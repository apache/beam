import google.auth
import json
from google.auth.transport import requests

# Todo: add pagination support to client
class DicomApiHttpClient:
  """DICOM api client that talk to api via http request"""
  healthcare_base_url = "https://healthcare.googleapis.com/v1"
  session = None
  
  def get_session(self, credential):
    if self.session:
      return self.session
    
    # if the credential is not provided, use the default credential.
    if not credential:
      credential, _ = google.auth.default()
    new_seesion = requests.AuthorizedSession(credential)
    self.session = new_seesion
    return new_seesion

  def qido_search(
    self, project_id, region, dataset_id, dicom_store_id, search_type, params=None, credential=None
  ):
    """function for searching a DICOM store"""

    # sending request to the REST healthcare api.
    api_endpoint = "{}/projects/{}/locations/{}".format(self.healthcare_base_url, project_id, region)

    # base of dicomweb path.
    dicomweb_path = "{}/datasets/{}/dicomStores/{}/dicomWeb/{}".format(
      api_endpoint, dataset_id, dicom_store_id, search_type
    )

    print(dicomweb_path)
    # Make an authenticated API request
    session = self.get_session(credential)
    headers = {"Content-Type": "application/dicom+json; charset=utf-8"}
    response = session.get(dicomweb_path, headers=headers, params=params)
    response.raise_for_status()
    status = response.status_code
    
    if status != 200:
      return [], status
    results = response.json()
    return results, status

  def dicomweb_store_instance(
    self, project_id, region, dataset_id, dicom_store_id, dcm_file, credential=None
  ):
    """function for storing an instance."""

    api_endpoint = "{}/projects/{}/locations/{}".format(self.healthcare_base_url, project_id, region)

    dicomweb_path = "{}/datasets/{}/dicomStores/{}/dicomWeb/studies".format(
      api_endpoint, dataset_id, dicom_store_id
    )

    # Make an authenticated API request
    session = self.get_session(credential)
    content_type = "application/dicom"
    headers = {"Content-Type": content_type}

    response = session.post(dicomweb_path, data=dcm_file, headers=headers)
    response.raise_for_status()
    
    return None, response.status_code