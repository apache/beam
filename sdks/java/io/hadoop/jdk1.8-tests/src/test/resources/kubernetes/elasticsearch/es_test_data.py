# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Script to populate data on Elasticsearch
# Hashcode for 1000 records is ed36c09b5e24a95fd8d3cc711a043a85320bb47d, 
# For test with query to select one record from 1000 docs, 
# hashcode is 83c108ff81e87b6f3807c638e6bb9a9e3d430dc7
# Hashcode for 50m records (~20 gigs) is aff7390ee25c4c330f0a58dfbfe335421b11e405 
#!/usr/bin/python

import json
import time
import logging
import random
import string
import uuid
import datetime

import tornado.gen
import tornado.httpclient
import tornado.ioloop
import tornado.options

async_http_client = tornado.httpclient.AsyncHTTPClient()
id_counter = 0
upload_data_count = 0
_dict_data = None



def delete_index(idx_name):
    try:
        url = "%s/%s?refresh=true" % (tornado.options.options.es_url, idx_name)
        request = tornado.httpclient.HTTPRequest(url, method="DELETE", request_timeout=240, 
                                                 auth_username=tornado.options.options.username, 
                                                 auth_password=tornado.options.options.password)
        response = tornado.httpclient.HTTPClient().fetch(request)
        logging.info('Deleting index  "%s" done   %s' % (idx_name, response.body))
    except tornado.httpclient.HTTPError:
        pass


def create_index(idx_name):
    schema = {
        "settings": {
            "number_of_shards":   tornado.options.options.num_of_shards,
            "number_of_replicas": tornado.options.options.num_of_replicas
        },
        "refresh": True
    }

    body = json.dumps(schema)
    url = "%s/%s" % (tornado.options.options.es_url, idx_name)
    try:
        logging.info('Trying to create index %s' % (url))
        request = tornado.httpclient.HTTPRequest(url, method="PUT", body=body, request_timeout=240,
                                                 auth_username=tornado.options.options.username, 
                                                 auth_password=tornado.options.options.password)
        response = tornado.httpclient.HTTPClient().fetch(request)
        logging.info('Creating index "%s" done   %s' % (idx_name, response.body))
    except tornado.httpclient.HTTPError:
        logging.info('Looks like the index exists already')
        pass


@tornado.gen.coroutine
def upload_batch(upload_data_txt):
    try:
        request = tornado.httpclient.HTTPRequest(tornado.options.options.es_url + "/_bulk",
                                                 method="POST", body=upload_data_txt,
                                                 request_timeout=
                                                 tornado.options.options.http_upload_timeout,
                                                 auth_username=tornado.options.options.username, 
                                                 auth_password=tornado.options.options.password)
        response = yield async_http_client.fetch(request)
    except Exception as ex:
        logging.error("upload failed, error: %s" % ex)
        return

    result = json.loads(response.body.decode('utf-8'))
    res_txt = "OK" if not result['errors'] else "FAILED"
    took = int(result['took'])
    logging.info("Upload: %s - upload took: %5dms, total docs uploaded: %7d" % (res_txt, took, 
                                                                                upload_data_count))


def get_data_for_format(format,count):
    split_f = format.split(":")
    if not split_f:
        return None, None

    field_name = split_f[0]
    field_type = split_f[1]

    return_val = ''

    if field_type == "bool":
        if count%2 == 0:
           return_val = True
        else:
           return_val = False

    elif field_type == "str":
        return_val = field_name + str(count)

    elif field_type == "int":
        return_val = count
    
    elif field_type == "ipv4":
        return_val = "{0}.{1}.{2}.{3}".format(1,2,3,count%255)

    elif field_type in ["ts", "tstxt"]:
        return_val = int(count * 1000) if field_type == "ts" else\
        			 datetime.datetime.fromtimestamp(count)\
        			 .strftime("%Y-%m-%dT%H:%M:%S.000-0000")

    elif field_type == "words":
        return_val = field_name + str(count)

    elif field_type == "dict":
        mydict = dict(a=field_name + str(count), b=field_name + str(count), c=field_name + str(count),
                      d=field_name + str(count), e=field_name + str(count), f=field_name + str(count),
                      g=field_name + str(count), h=field_name + str(count), i=field_name + str(count), 
                      j=field_name + str(count))
        return_val = ", ".join("=".join(_) for _ in mydict.items())

    elif field_type == "text":
        return_val = field_name + str(count)

    return field_name, return_val


def generate_count(min, max):
    if min == max:
        return max
    elif min > max:
        return random.randrange(max, min);
    else:
        return random.randrange(min, max);


def generate_random_doc(format,count):
    global id_counter

    res = {}

    for f in format:
        f_key, f_val = get_data_for_format(f,count)
        if f_key:
            res[f_key] = f_val

    if not tornado.options.options.id_type:
        return res

    if tornado.options.options.id_type == 'int':
        res['_id'] = id_counter
        id_counter += 1
    elif tornado.options.options.id_type == 'uuid4':
        res['_id'] = str(uuid.uuid4())

    return res


def set_index_refresh(val):

    params = {"index": {"refresh_interval": val}}
    body = json.dumps(params)
    url = "%s/%s/_settings" % (tornado.options.options.es_url, tornado.options.options.index_name)
    try:
        request = tornado.httpclient.HTTPRequest(url, method="PUT", body=body, request_timeout=240,
                                                 auth_username=tornado.options.options.username, 
                                                 auth_password=tornado.options.options.password)
        http_client = tornado.httpclient.HTTPClient()
        http_client.fetch(request)
        logging.info('Set index refresh to %s' % val)
    except Exception as ex:
        logging.exception(ex)


@tornado.gen.coroutine
def generate_test_data():

    global upload_data_count

    if tornado.options.options.force_init_index:
        delete_index(tornado.options.options.index_name)

    create_index(tornado.options.options.index_name)

    # todo: query what refresh is set to, then restore later
    if tornado.options.options.set_refresh:
        set_index_refresh("-1")

    if tornado.options.options.out_file:
        out_file = open(tornado.options.options.out_file, "w")
    else:
        out_file = None

    if tornado.options.options.dict_file:
        global _dict_data
        with open(tornado.options.options.dict_file, 'r') as f:
            _dict_data = f.readlines()
        logging.info("Loaded %d words from the %s" % (len(_dict_data), 
                                                      tornado.options.options.dict_file))

    format = tornado.options.options.format.split(',')
    if not format:
        logging.error('invalid format')
        exit(1)

    ts_start = int(time.time())
    upload_data_txt = ""
    total_uploaded = 0

    logging.info("Generating %d docs, upload batch size is %d" % (tornado.options.options.count,
                                                                  tornado.options
                                                                  .options.batch_size))
    for num in range(0, tornado.options.options.count):

        item = generate_random_doc(format,num)

        if out_file:
            out_file.write("%s\n" % json.dumps(item))

        cmd = {'index': {'_index': tornado.options.options.index_name,
                         '_type': tornado.options.options.index_type}}
        if '_id' in item:
            cmd['index']['_id'] = item['_id']

        upload_data_txt += json.dumps(cmd) + "\n"
        upload_data_txt += json.dumps(item) + "\n"
        upload_data_count += 1

        if upload_data_count % tornado.options.options.batch_size == 0:
            yield upload_batch(upload_data_txt)
            upload_data_txt = ""

    # upload remaining items in `upload_data_txt`
    if upload_data_txt:
        yield upload_batch(upload_data_txt)

    if tornado.options.options.set_refresh:
        set_index_refresh("1s")

    if out_file:
        out_file.close()

    took_secs = int(time.time() - ts_start)

    logging.info("Done - total docs uploaded: %d, took %d seconds" % 
    			 (tornado.options.options.count, took_secs))


if __name__ == '__main__':
    tornado.options.define("es_url", type=str, default='http://localhost:9200/', 
                           help="URL of your Elasticsearch node")
    tornado.options.define("index_name", type=str, default='test_data', 
                           help="Name of the index to store your messages")
    tornado.options.define("index_type", type=str, default='test_type', help="Type")
    tornado.options.define("batch_size", type=int, default=1000, 
                           help="Elasticsearch bulk index batch size")
    tornado.options.define("num_of_shards", type=int, default=2, 
                           help="Number of shards for ES index")
    tornado.options.define("http_upload_timeout", type=int, default=3, 
                           help="Timeout in seconds when uploading data")
    tornado.options.define("count", type=int, default=100000, help="Number of docs to generate")
    tornado.options.define("format", type=str, default='name:str,age:int,last_updated:ts', 
                           help="message format")
    tornado.options.define("num_of_replicas", type=int, default=0, 
                           help="Number of replicas for ES index")
    tornado.options.define("force_init_index", type=bool, default=False, 
                           help="Force deleting and re-initializing the Elasticsearch index")
    tornado.options.define("set_refresh", type=bool, default=False, 
                           help="Set refresh rate to -1 before starting the upload")
    tornado.options.define("out_file", type=str, default=False, 
                           help="If set, write test data to out_file as well.")
    tornado.options.define("id_type", type=str, default=None, 
                           help="Type of 'id' to use for the docs, \
                           valid settings are int and uuid4, None is default")
    tornado.options.define("dict_file", type=str, default=None, 
                           help="Name of dictionary file to use")
    tornado.options.define("username", type=str, default=None, help="Username for elasticsearch")
    tornado.options.define("password", type=str, default=None, help="Password for elasticsearch")
    tornado.options.parse_command_line()

    tornado.ioloop.IOLoop.instance().run_sync(generate_test_data)
