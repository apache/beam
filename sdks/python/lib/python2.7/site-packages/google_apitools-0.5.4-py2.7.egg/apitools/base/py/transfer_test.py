# -*- coding: utf-8 -*-
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

"""Tests for transfer.py."""
import string

import mock
import six
from six.moves import http_client
import unittest2

from apitools.base.py import base_api
from apitools.base.py import http_wrapper
from apitools.base.py import transfer


class TransferTest(unittest2.TestCase):

    def assertRangeAndContentRangeCompatible(self, request, response):
        request_prefix = 'bytes='
        self.assertIn('range', request.headers)
        self.assertTrue(request.headers['range'].startswith(request_prefix))
        request_range = request.headers['range'][len(request_prefix):]

        response_prefix = 'bytes '
        self.assertIn('content-range', response.info)
        response_header = response.info['content-range']
        self.assertTrue(response_header.startswith(response_prefix))
        response_range = (
            response_header[len(response_prefix):].partition('/')[0])

        msg = ('Request range ({0}) not a prefix of '
               'response_range ({1})').format(
                   request_range, response_range)
        self.assertTrue(response_range.startswith(request_range), msg=msg)

    def testComputeEndByte(self):
        total_size = 100
        chunksize = 10
        download = transfer.Download.FromStream(
            six.StringIO(), chunksize=chunksize, total_size=total_size)
        self.assertEqual(chunksize - 1,
                         download._Download__ComputeEndByte(0, end=50))

    def testComputeEndByteReturnNone(self):
        download = transfer.Download.FromStream(six.StringIO())
        self.assertIsNone(
            download._Download__ComputeEndByte(0, use_chunks=False))

    def testComputeEndByteNoChunks(self):
        total_size = 100
        download = transfer.Download.FromStream(
            six.StringIO(), chunksize=10, total_size=total_size)
        for end in (None, 1000):
            self.assertEqual(
                total_size - 1,
                download._Download__ComputeEndByte(0, end=end,
                                                   use_chunks=False),
                msg='Failed on end={0}'.format(end))

    def testComputeEndByteNoTotal(self):
        download = transfer.Download.FromStream(six.StringIO())
        default_chunksize = download.chunksize
        for chunksize in (100, default_chunksize):
            download.chunksize = chunksize
            for start in (0, 10):
                self.assertEqual(
                    download.chunksize + start - 1,
                    download._Download__ComputeEndByte(start),
                    msg='Failed on start={0}, chunksize={1}'.format(
                        start, chunksize))

    def testComputeEndByteSmallTotal(self):
        total_size = 100
        download = transfer.Download.FromStream(six.StringIO(),
                                                total_size=total_size)
        for start in (0, 10):
            self.assertEqual(total_size - 1,
                             download._Download__ComputeEndByte(start),
                             msg='Failed on start={0}'.format(start))

    def testGetRange(self):
        for (start_byte, end_byte) in [(0, 25), (5, 15), (0, 0), (25, 25)]:
            bytes_http = object()
            http = object()
            download_stream = six.StringIO()
            download = transfer.Download.FromStream(download_stream,
                                                    total_size=26,
                                                    auto_transfer=False)
            download.bytes_http = bytes_http
            base_url = 'https://part.one/'
            with mock.patch.object(http_wrapper, 'MakeRequest',
                                   autospec=True) as make_request:
                make_request.return_value = http_wrapper.Response(
                    info={
                        'content-range': 'bytes %d-%d/26' %
                                         (start_byte, end_byte),
                        'status': http_client.OK,
                    },
                    content=string.ascii_lowercase[start_byte:end_byte+1],
                    request_url=base_url,
                )
                request = http_wrapper.Request(url='https://part.one/')
                download.InitializeDownload(request, http=http)
                download.GetRange(start_byte, end_byte)
                self.assertEqual(1, make_request.call_count)
                received_request = make_request.call_args[0][1]
                self.assertEqual(base_url, received_request.url)
                self.assertRangeAndContentRangeCompatible(
                    received_request, make_request.return_value)

    def testNonChunkedDownload(self):
        bytes_http = object()
        http = object()
        download_stream = six.StringIO()
        download = transfer.Download.FromStream(download_stream, total_size=52)
        download.bytes_http = bytes_http
        base_url = 'https://part.one/'

        with mock.patch.object(http_wrapper, 'MakeRequest',
                               autospec=True) as make_request:
            make_request.return_value = http_wrapper.Response(
                info={
                    'content-range': 'bytes 0-51/52',
                    'status': http_client.OK,
                },
                content=string.ascii_lowercase * 2,
                request_url=base_url,
            )
            request = http_wrapper.Request(url='https://part.one/')
            download.InitializeDownload(request, http=http)
            self.assertEqual(1, make_request.call_count)
            received_request = make_request.call_args[0][1]
            self.assertEqual(base_url, received_request.url)
            self.assertRangeAndContentRangeCompatible(
                received_request, make_request.return_value)
            download_stream.seek(0)
            self.assertEqual(string.ascii_lowercase * 2,
                             download_stream.getvalue())

    def testChunkedDownload(self):
        bytes_http = object()
        http = object()
        download_stream = six.StringIO()
        download = transfer.Download.FromStream(
            download_stream, chunksize=26, total_size=52)
        download.bytes_http = bytes_http

        # Setting autospec on a mock with an iterable side_effect is
        # currently broken (http://bugs.python.org/issue17826), so
        # instead we write a little function.
        def _ReturnBytes(unused_http, http_request,
                         *unused_args, **unused_kwds):
            url = http_request.url
            if url == 'https://part.one/':
                return http_wrapper.Response(
                    info={
                        'content-location': 'https://part.two/',
                        'content-range': 'bytes 0-25/52',
                        'status': http_client.PARTIAL_CONTENT,
                    },
                    content=string.ascii_lowercase,
                    request_url='https://part.one/',
                )
            elif url == 'https://part.two/':
                return http_wrapper.Response(
                    info={
                        'content-range': 'bytes 26-51/52',
                        'status': http_client.OK,
                    },
                    content=string.ascii_uppercase,
                    request_url='https://part.two/',
                )
            else:
                self.fail('Unknown URL requested: %s' % url)

        with mock.patch.object(http_wrapper, 'MakeRequest',
                               autospec=True) as make_request:
            make_request.side_effect = _ReturnBytes
            request = http_wrapper.Request(url='https://part.one/')
            download.InitializeDownload(request, http=http)
            self.assertEqual(2, make_request.call_count)
            for call in make_request.call_args_list:
                self.assertRangeAndContentRangeCompatible(
                    call[0][1], _ReturnBytes(*call[0]))
            download_stream.seek(0)
            self.assertEqual(string.ascii_lowercase + string.ascii_uppercase,
                             download_stream.getvalue())

    def testMultipartEncoding(self):
        # This is really a table test for various issues we've seen in
        # the past; see notes below for particular histories.

        test_cases = [
            # Python's mime module by default encodes lines that start
            # with "From " as ">From ", which we need to make sure we
            # don't run afoul of when sending content that isn't
            # intended to be so encoded. This test calls out that we
            # get this right. We test for both the multipart and
            # non-multipart case.
            'line one\nFrom \nline two',

            # We had originally used a `six.StringIO` to hold the http
            # request body in the case of a multipart upload; for
            # bytes being uploaded in Python3, however, this causes
            # issues like this:
            # https://github.com/GoogleCloudPlatform/gcloud-python/issues/1760
            # We test below to ensure that we don't end up mangling
            # the body before sending.
            u'name,main_ingredient\nRäksmörgås,Räkor\nBaguette,Bröd',
        ]

        for upload_contents in test_cases:
            multipart_body = '{"body_field_one": 7}'
            upload_bytes = upload_contents.encode('ascii', 'backslashreplace')
            upload_config = base_api.ApiUploadInfo(
                accept=['*/*'],
                max_size=None,
                resumable_multipart=True,
                resumable_path=u'/resumable/upload',
                simple_multipart=True,
                simple_path=u'/upload',
            )
            url_builder = base_api._UrlBuilder('http://www.uploads.com')

            # Test multipart: having a body argument in http_request forces
            # multipart here.
            upload = transfer.Upload.FromStream(
                six.BytesIO(upload_bytes),
                'text/plain',
                total_size=len(upload_bytes))
            http_request = http_wrapper.Request(
                'http://www.uploads.com',
                headers={'content-type': 'text/plain'},
                body=multipart_body)
            upload.ConfigureRequest(upload_config, http_request, url_builder)
            self.assertEqual(
                'multipart', url_builder.query_params['uploadType'])
            rewritten_upload_contents = b'\n'.join(
                http_request.body.split(b'--')[2].splitlines()[1:])
            self.assertTrue(rewritten_upload_contents.endswith(upload_bytes))

            # Test non-multipart (aka media): no body argument means this is
            # sent as media.
            upload = transfer.Upload.FromStream(
                six.BytesIO(upload_bytes),
                'text/plain',
                total_size=len(upload_bytes))
            http_request = http_wrapper.Request(
                'http://www.uploads.com',
                headers={'content-type': 'text/plain'})
            upload.ConfigureRequest(upload_config, http_request, url_builder)
            self.assertEqual(url_builder.query_params['uploadType'], 'media')
            rewritten_upload_contents = http_request.body
            self.assertTrue(rewritten_upload_contents.endswith(upload_bytes))
