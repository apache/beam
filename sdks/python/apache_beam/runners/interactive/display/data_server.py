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
from __future__ import absolute_import

import queue
import sys
import threading
import time

try:
  from weakref import finalize
except ImportError:
  from backports.weakref import finalize

__all__ = ["create_data_publisher_app", "ServerThread"]


class StreamingPlot(object):

  def __init__(self,
               caches,
               processors,
               plot_fn,
               source_type="column",
               **reader_kwargs):
    if source_type not in ["column", "ajax"]:
      raise ValueError(
          "column and ajax are the only two source types that are supported.")
    if not isinstance(caches, (tuple, list)):
      caches = [caches]
    if not isinstance(processors, (tuple, list)):
      processors = [processors] * len(caches)
    self.caches = caches
    self.processors = processors
    self.plot_fn = plot_fn
    self.reader_kwargs = reader_kwargs
    if source_type == "column":
      self.worker_class = CacheToColumnSourceWorker
    else:
      self.worker_class = CacheToAjaxSourceWorker
    self._reset_state()

  def _reset_state(self):
    self.sources = []
    self.handle = None
    self._threads = []
    self._finalizer = finalize(
        self, lambda threads: [thread.stop() for thread in threads],
        self._threads)

  def start(self):
    import bokeh

    for cache, processor in zip(self.caches, self.processors):
      thread = self.worker_class(cache, processor, **self.reader_kwargs)
      thread.start()
      self._threads.append(thread)
    for thread in self._threads:
      self.sources.append(thread.source)
    plot = self.plot_fn(*self.sources)
    self.handle = bokeh.io.show(plot, notebook_handle=True)
    for thread in self._threads:
      thread.handle = self.handle
    return plot

  def stop(self):
    self._finalizer()
    self._reset_state()


class CacheToColumnSourceWorker(threading.Thread):

  def __init__(self, cache, processor, **reader_kwargs):
    super().__init__(daemon=True)
    self.cache = cache
    self.processor = processor
    self.reader_kwargs = reader_kwargs
    self._source = None
    self._source_event = threading.Event()
    self._handle = None
    self._handle_event = threading.Event()
    self._stop_event = threading.Event()

  def run(self):
    import bokeh

    reader_kwargs = self.reader_kwargs.copy()

    rollover = reader_kwargs.pop("rollover", None)
    for timestamped_element in self.cache.read(**reader_kwargs):
      data = self.processor(timestamped_element)
      if self._source is None:
        self._source = bokeh.models.ColumnDataSource(data)
        self._source_event.set()
      else:
        self._source.stream(data, rollover=rollover)
      if self._handle_event.is_set():
        bokeh.io.push_notebook(handle=self.handle)
      if self._stop_event.is_set():
        break

  def stop(self):
    self._stop_event.set()

  @property
  def handle(self):
    return self._handle

  @handle.setter
  def handle(self, handle):
    if self._handle is not None:
      raise ValueError("The handle can only be set once!")
    self._handle = handle
    self._handle_event.set()

  @property
  def source(self):
    self._source_event.wait()
    return self._source


class CacheToAjaxSourceWorker(threading.Thread):

  def __init__(self,
               cache,
               processor,
               host="localhost",
               remote_host="localhost",
               port=0,
               **reader_kwargs):
    super().__init__(daemon=True)
    self.cache = cache
    self.processor = processor
    self.host = host
    self.remote_host = remote_host
    self.port = port
    self.reader_kwargs = reader_kwargs

    self.server = None
    self.context = None
    self._finalizer = finalize(self, lambda: None)

    self._source = None
    self._source_event = threading.Event()
    self._handle = None
    self._stop_event = threading.Event()

  def run(self):
    import bokeh
    from werkzeug.serving import make_server

    reader_kwargs = self.reader_kwargs.copy()

    delay = reader_kwargs.pop("delay", 0)
    timeout = reader_kwargs.pop("timeout", 1)
    polling_interval = reader_kwargs.pop("polling_interval", 500)
    with self.cache.read_to_queue(**reader_kwargs) as data_queue:
      time.sleep(delay)
      app = create_data_publisher_app(
          data_queue, processor=self.processor, timeout=timeout)
      self.server = make_server(
          host=self.host, port=self.port, app=app, threaded=False)
      self._finalizer = finalize(self, lambda server: server.shutdown(),
                                 self.server)
      self.context = app.app_context()
      self.context.push()

      data_url = "http://{}:{}/data".format(self.remote_host, self.server.port)
      self._source = bokeh.models.AjaxDataSource(
          data_url=data_url,
          polling_interval=polling_interval,
          mode="append",  # adapter=adapter,
      )
      self._source_event.set()

      self.server.serve_forever()

  def stop(self):
    self._finalizer()

  @property
  def handle(self):
    return self._handle

  @handle.setter
  def handle(self, handle):
    self._handle = handle

  @property
  def source(self):
    self._source_event.wait()
    return self._source


def create_data_publisher_app(data_queue, processor=None, timeout=1):
  """Create a flask app that can serve data from the data_queue.

  Args:
      data_queue (queue.Queue): A source of data conforming to the Queue API.
      processors (List[callable]): A list of functions that will be applied to
          the data before serving.
      timeout (float): Time (in seconds) that we should wait when obtaining
          an element from the queue.

  .. warning::
      data_queue should probably be thread-safe, or strange things might
      happen.
  """
  import flask
  app = flask.Flask(__name__)
  keys = []

  def crossdomain(f):
    """Allow access to the endpoint from different ports and domains."""

    def wrapped_function(*args, **kwargs):
      resp = flask.make_response(f(*args, **kwargs))
      h = resp.headers
      h["Access-Control-Allow-Origin"] = "*"
      h["Access-Control-Allow-Methods"] = "GET, OPTIONS, POST"
      h["Access-Control-Max-Age"] = str(21600)
      requested_headers = flask.request.headers.get(
          "Access-Control-Request-Headers")
      if requested_headers:
        h["Access-Control-Allow-Headers"] = requested_headers

      return resp

    return wrapped_function

  # TODO(ostrokach): Maybe should randomize the URL instead of using data,
  # in case anyone ever exposes this to the internet.
  @app.route("/data", methods=["GET", "OPTIONS", "POST"])
  @crossdomain
  def data():  # pylint: disable=unused-variable
    try:
      data = data_queue.get(timeout=timeout)
    except queue.Empty:
      data = {k: [] for k in keys}
    else:
      if processor is not None:
        data = processor(data)
    if not keys:
      for key in data:
        keys.append(key)
    return flask.jsonify(data)

  return app


class ServerThread(threading.Thread):
  """Serve the WSGI application app."""

  # Credit: https://stackoverflow.com/a/45017691/2063031

  def __init__(self, app, host="localhost", port=0, threaded=False):
    from werkzeug.serving import make_server
    super(ServerThread, self).__init__(daemon=True)
    self.server = make_server(host=host, port=port, app=app, threaded=threaded)
    self.context = app.app_context()
    self.context.push()
    if sys.version_info > (3, 4):
      self._finalizer = finalize(self, lambda server: server.shutdown(),
                                 self.server)
    else:
      self._finalizer = self.server.shutdown

  def run(self):
    self.server.serve_forever()

  def stop(self):
    self._finalizer()

  def __enter__(self):
    self.start()
    return self

  def __exit__(self, *args):
    self.stop()


# class BokehThread(threading.Thread):

#   def __init__(self,
#                data_source,
#                data_sink,
#                plot_handle,
#                processors=None,
#                timeout=1,
#                rollover=None):
#     self._data_source = data_source
#     self._plot_handle = plot_handle
#     self._processors = processors
#     self._timeout = timeout
#     self._rollover = None
#     self._data_sink = data_sink

#   def run(self):
#     while True:
#       element = self._data_source.pull(timeout=self._timeout)
#       if self._processors is not None:
#         for processor in self._processors:
#           element = processor(element)
#         self._data_sink.stream(element, rollover=self._rollover)
