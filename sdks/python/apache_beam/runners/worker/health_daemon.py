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

import http.client
import logging
import time
import socket
import errno

from builtins import object

class HealthDaemon(object):
    @staticmethod
    def connect_to_server(health_http_port, timeout=5):
        logging.info('Connecting to localhost:{}'.format(health_http_port))
        return http.client.HTTPConnection('localhost', health_http_port, timeout=timeout)

    @staticmethod
    def try_health_ping(health_server):
        success = False
        try:
            health_server.request('PUT', '/sdk')
            resp = health_server.getresponse()
            if resp.status == 200:
                logging.info('Successfully sent health ping to localhost:{}'.format(health_server.port))
                success = True
            else:
                logging.warning('Failed to send health ping to localhost:{} with: HTTP {} {}'.format(
                    health_server.port, resp.status, resp.reason))

            # Flush the response to close the connection.
            resp.read()
        except http.client.HTTPException as e:
            logging.error('Could not send health ping to localhost:{} with exception: {}'.format(
                health_server.port, e))
        except socket.error as e:
            if e.errno == errno.ECONNREFUSED:
                logging.error('Connection refused by server')
            health_server.close()
        except:
            logging.error('Unknown error while trying to send health ping. Continuing.')
            health_server.close()
        return success

    @staticmethod
    def start(health_http_port=8080):
        """Executes the serving loop for the status server.

        Args:
          health_http_port(int): Binding port for the debug server.
            Default is 0 which means any free unsecured port
        """
        conn = HealthDaemon.connect_to_server(health_http_port)
        while True:
            HealthDaemon.try_health_ping(conn)

            logging.info('Health Client Daemon sleeping for 15 seconds...')
            time.sleep(15)