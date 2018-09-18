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

import http.server
import json
import socketserver
from postcommit_query import BeamPostCommitQuery
from expiringdict import ExpiringDict

PORT = 8000
beam_query = BeamPostCommitQuery()
cache = ExpiringDict(max_len=10, max_age_seconds=180, default={})

class MyHandler(http.server.SimpleHTTPRequestHandler):
  def do_POST(self):
    data = cache.get('data')
    if not data:
      print('Getting new data')
      data = beam_query.getData()
      cache['data'] = data
    else:
      print('Using cached data')
    self.protocol_version = 'HTTP/1.1'
    self.send_response(200)
    self.send_header('Content-Type', 'application/json')
    self.send_header('Content-Length', len(data.encode('utf-8')))
    self.end_headers()
    self.wfile.write(data.encode('utf-8'))

def run():
  httpd = socketserver.TCPServer(("0.0.0.0", PORT), MyHandler)
  print("serving at port", PORT)
  httpd.serve_forever()

if __name__ == "__main__":
    cache['data'] = beam_query.getData()
    run()
