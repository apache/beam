#
# Copyright 2015 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Modified generated client library for fusiontables version v1.

This is a hand-customized and pruned version of the fusiontables v1
client, designed for use in testing.

"""

from apitools.base.py import base_api
from apitools.base.py.testing.testclient import fusiontables_v1_messages


class FusiontablesV1(base_api.BaseApiClient):

    """Generated client library for service fusiontables version v1."""

    MESSAGES_MODULE = fusiontables_v1_messages

    _PACKAGE = u'fusiontables'
    _SCOPES = [u'https://www.googleapis.com/auth/fusiontables',
               u'https://www.googleapis.com/auth/fusiontables.readonly']
    _VERSION = u'v1'
    _CLIENT_ID = '1042881264118.apps.googleusercontent.com'
    _CLIENT_SECRET = 'x_Tw5K8nnjoRAqULM9PFAC2b'
    _USER_AGENT = ''
    _CLIENT_CLASS_NAME = u'FusiontablesV1'
    _URL_VERSION = u'v1'

    def __init__(self, url='', credentials=None,
                 get_credentials=True, http=None, model=None,
                 log_request=False, log_response=False,
                 credentials_args=None, default_global_params=None,
                 additional_http_headers=None):
        """Create a new fusiontables handle."""
        url = url or u'https://www.googleapis.com/fusiontables/v1/'
        super(FusiontablesV1, self).__init__(
            url, credentials=credentials,
            get_credentials=get_credentials, http=http, model=model,
            log_request=log_request, log_response=log_response,
            credentials_args=credentials_args,
            default_global_params=default_global_params,
            additional_http_headers=additional_http_headers)
        self.column = self.ColumnService(self)
        self.columnalternate = self.ColumnAlternateService(self)

    class ColumnService(base_api.BaseApiService):

        """Service class for the column resource."""

        _NAME = u'column'

        def __init__(self, client):
            super(FusiontablesV1.ColumnService, self).__init__(client)
            self._method_configs = {
                'List': base_api.ApiMethodInfo(
                    http_method=u'GET',
                    method_id=u'fusiontables.column.list',
                    ordered_params=[u'tableId'],
                    path_params=[u'tableId'],
                    query_params=[u'maxResults', u'pageToken'],
                    relative_path=u'tables/{tableId}/columns',
                    request_field='',
                    request_type_name=u'FusiontablesColumnListRequest',
                    response_type_name=u'ColumnList',
                    supports_download=False,
                ),
            }

            self._upload_configs = {
            }

        def List(self, request, global_params=None):
            """Retrieves a list of columns.

            Args:
              request: (FusiontablesColumnListRequest) input message
              global_params: (StandardQueryParameters, default: None) global
                  arguments
            Returns:
              (ColumnList) The response message.
            """
            config = self.GetMethodConfig('List')
            return self._RunMethod(
                config, request, global_params=global_params)

    class ColumnAlternateService(base_api.BaseApiService):

        """Service class for the column resource."""

        _NAME = u'columnalternate'

        def __init__(self, client):
            super(FusiontablesV1.ColumnAlternateService, self).__init__(client)
            self._method_configs = {
                'List': base_api.ApiMethodInfo(
                    http_method=u'GET',
                    method_id=u'fusiontables.column.listalternate',
                    ordered_params=[u'tableId'],
                    path_params=[u'tableId'],
                    query_params=[u'maxResults', u'pageToken'],
                    relative_path=u'tables/{tableId}/columns',
                    request_field='',
                    request_type_name=(
                        u'FusiontablesColumnListAlternateRequest'),
                    response_type_name=u'ColumnListAlternate',
                    supports_download=False,
                ),
            }

            self._upload_configs = {
            }

        def List(self, request, global_params=None):
            """Retrieves a list of columns.

            Args:
              request: (FusiontablesColumnListRequest) input message
              global_params: (StandardQueryParameters, default: None) global
                  arguments
            Returns:
              (ColumnList) The response message.
            """
            config = self.GetMethodConfig('List')
            return self._RunMethod(
                config, request, global_params=global_params)
