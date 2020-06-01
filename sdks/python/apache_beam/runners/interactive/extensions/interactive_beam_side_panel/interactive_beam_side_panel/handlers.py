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

import json

import tornado
from notebook.base.handlers import APIHandler
from notebook.utils import url_path_join


class RouteHandler(APIHandler):
  # The following decorator should be present on all verb methods (head, get, post,
  # patch, put, delete, options) to ensure only authorized user can request the
  # Jupyter server
  @tornado.web.authenticated
  def get(self):
    self.finish(
        json.dumps({
            "data": "This is /interactive_beam_side_panel/get_example endpoint!"
        }))


def setup_handlers(web_app):
  host_pattern = ".*$"

  base_url = web_app.settings["base_url"]
  route_pattern = url_path_join(
      base_url, "interactive_beam_side_panel", "get_example")
  handlers = [(route_pattern, RouteHandler)]
  web_app.add_handlers(host_pattern, handlers)
