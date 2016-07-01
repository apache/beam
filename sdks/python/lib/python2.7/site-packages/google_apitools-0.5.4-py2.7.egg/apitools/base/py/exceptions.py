#!/usr/bin/env python
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

"""Exceptions for generated client libraries."""


class Error(Exception):

    """Base class for all exceptions."""


class TypecheckError(Error, TypeError):

    """An object of an incorrect type is provided."""


class NotFoundError(Error):

    """A specified resource could not be found."""


class UserError(Error):

    """Base class for errors related to user input."""


class InvalidDataError(Error):

    """Base class for any invalid data error."""


class CommunicationError(Error):

    """Any communication error talking to an API server."""


class HttpError(CommunicationError):

    """Error making a request. Soon to be HttpError."""

    def __init__(self, response, content, url):
        super(HttpError, self).__init__()
        self.response = response
        self.content = content
        self.url = url

    def __str__(self):
        content = self.content
        if isinstance(content, bytes):
            content = self.content.decode('ascii', 'replace')
        return 'HttpError accessing <%s>: response: <%s>, content <%s>' % (
            self.url, self.response, content)

    @property
    def status_code(self):
        # TODO(craigcitro): Turn this into something better than a
        # KeyError if there is no status.
        return int(self.response['status'])

    @classmethod
    def FromResponse(cls, http_response):
        return cls(http_response.info, http_response.content,
                   http_response.request_url)


class InvalidUserInputError(InvalidDataError):

    """User-provided input is invalid."""


class InvalidDataFromServerError(InvalidDataError, CommunicationError):

    """Data received from the server is malformed."""


class BatchError(Error):

    """Error generated while constructing a batch request."""


class ConfigurationError(Error):

    """Base class for configuration errors."""


class GeneratedClientError(Error):

    """The generated client configuration is invalid."""


class ConfigurationValueError(UserError):

    """Some part of the user-specified client configuration is invalid."""


class ResourceUnavailableError(Error):

    """User requested an unavailable resource."""


class CredentialsError(Error):

    """Errors related to invalid credentials."""


class TransferError(CommunicationError):

    """Errors related to transfers."""


class TransferRetryError(TransferError):

    """Retryable errors related to transfers."""


class TransferInvalidError(TransferError):

    """The given transfer is invalid."""


class RequestError(CommunicationError):

    """The request was not successful."""


class RetryAfterError(HttpError):

    """The response contained a retry-after header."""

    def __init__(self, response, content, url, retry_after):
        super(RetryAfterError, self).__init__(response, content, url)
        self.retry_after = int(retry_after)

    @classmethod
    def FromResponse(cls, http_response):
        return cls(http_response.info, http_response.content,
                   http_response.request_url, http_response.retry_after)


class BadStatusCodeError(HttpError):

    """The request completed but returned a bad status code."""


class NotYetImplementedError(GeneratedClientError):

    """This functionality is not yet implemented."""


class StreamExhausted(Error):

    """Attempted to read more bytes from a stream than were available."""
