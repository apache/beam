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

import errno
import http.client
import logging
import socket
import time
from builtins import object


class HealthDaemon(object):
  """Sends periodic HTTP PUT /sdk requests to the health server.

  The purpose of this class is to communicate to the health server that this
  SDK Harness is alive. If this SDK Harness does not communicate to the health
  server after a configured amount of time, the health server will restart the
  container.

  Expected Usage:
    # The HealthDaemon is expected to spin forever, start it on a separate
    # thread.
    health_thread = threading.Thread(target=HealthDaemon(8080).start)

    # Automatically kill the thread when the program exists.
    health_thread.daemon = True
    health_thread.setName('health-client-demon')

    # Start the HealthDaemon.
    health_thread.start()

  """

  def __init__(self, health_http_port):
    self._health_http_port = health_http_port

  @staticmethod
  def connect_to_server(health_http_port, timeout=5):
    """Connects to the health server on the given port.

    Args:
      health_http_port(int): Binding port for the debug server.
        Default is 0 which means any free unsecured port
      timeout(int): Timeout in seconds for all operations.

    Returns:
      The connection to the health server.
    """

    logging.info('Connecting to localhost:%s', health_http_port)
    return http.client.HTTPConnection('localhost', health_http_port,
                                      timeout=timeout)

  @staticmethod
  def try_health_ping(health_server):
    """Attempts to ping the given health server.

    Args:
      health_server(http.client.HTTPConnection): Connection to the health
        server.

    Returns:
      True if the health ping succeeded, false otherwise.
    """

    success = False
    try:
      health_server.request('PUT', '/sdk')
      resp = health_server.getresponse()
      if resp.status == 200:
        logging.info('Successfully sent health ping to localhost:%s',
                     health_server.port)
        success = True
      else:
        logging.warning(('Failed to send health ping to localhost:%s with: '
                         'HTTP %s %s'),
                        health_server.port, resp.status, resp.reason)

      # Flush the response to close the connection.
      resp.read()
    except http.client.HTTPException as e:
      logging.error(('Could not send health ping to localhost:%s with '
                     'exception: %s'),
                    health_server.port, e)
    except socket.error as e:
      if e.errno == errno.ECONNREFUSED:
        logging.error('Connection refused by server')
      health_server.close()

    # We want the HealthDaemon to always try to ping, otherwise the container
    # will be shut down.
    except Exception as e:
      logging.error('Unknown error while trying to send health ping: %s', e)
      health_server.close()
    return success

  def start(self):
    """Tries forever to send a health ping to the health server."""

    conn = HealthDaemon.connect_to_server(self._health_http_port)
    while True:
      HealthDaemon.try_health_ping(conn)

      logging.info('Health Client Daemon sleeping for 15 seconds...')
      time.sleep(15)
