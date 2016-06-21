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

"""Command-line utility for fetching/inspecting credentials.

oauth2l (pronounced "oauthtool") is a small utility for fetching
credentials, or inspecting existing credentials. Here we demonstrate
some sample use:

    $ oauth2l fetch userinfo.email bigquery compute
    Fetched credentials of type:
      oauth2client.client.OAuth2Credentials
    Access token:
      ya29.abcdefghijklmnopqrstuvwxyz123yessirree
    $ oauth2l header userinfo.email
    Authorization: Bearer ya29.zyxwvutsrqpnmolkjihgfedcba
    $ oauth2l validate thisisnotatoken
    <exit status: 1>
    $ oauth2l validate ya29.zyxwvutsrqpnmolkjihgfedcba
    $ oauth2l scopes ya29.abcdefghijklmnopqrstuvwxyz123yessirree
    https://www.googleapis.com/auth/bigquery
    https://www.googleapis.com/auth/compute
    https://www.googleapis.com/auth/userinfo.email

The `header` command is designed to be easy to use with `curl`:

    $ curl -H "$(oauth2l header bigquery)" \\
      'https://www.googleapis.com/bigquery/v2/projects'
    ... lists all projects ...

The token can also be printed in other formats, for easy chaining
into other programs:

    $ oauth2l fetch -f json_compact userinfo.email
    <one-line JSON object with credential information>
    $ oauth2l fetch -f bare drive
    ya29.suchT0kenManyCredentialsW0Wokyougetthepoint

"""

from __future__ import print_function

import argparse
import json
import logging
import os
import pkgutil
import sys
import textwrap

import oauth2client.client
from six.moves import http_client

import apitools.base.py as apitools_base

# We could use a generated client here, but it's used for precisely
# one URL, with one parameter and no worries about URL encoding. Let's
# go with simple.
_OAUTH2_TOKENINFO_TEMPLATE = (
    'https://www.googleapis.com/oauth2/v2/tokeninfo'
    '?access_token={access_token}'
)


def GetDefaultClientInfo():
    client_secrets_json = pkgutil.get_data(
        'apitools.data', 'apitools_client_secrets.json').decode('utf8')
    client_secrets = json.loads(client_secrets_json)['installed']
    return {
        'client_id': client_secrets['client_id'],
        'client_secret': client_secrets['client_secret'],
        'user_agent': 'apitools/0.2 oauth2l/0.1',
    }


def GetClientInfoFromFlags(client_secrets):
    """Fetch client info from args."""
    if client_secrets:
        client_secrets_path = os.path.expanduser(client_secrets)
        if not os.path.exists(client_secrets_path):
            raise ValueError(
                'Cannot find file: {0}'.format(client_secrets))
        with open(client_secrets_path) as client_secrets_file:
            client_secrets = json.load(client_secrets_file)
        if 'installed' not in client_secrets:
            raise ValueError('Provided client ID must be for an installed app')
        client_secrets = client_secrets['installed']
        return {
            'client_id': client_secrets['client_id'],
            'client_secret': client_secrets['client_secret'],
            'user_agent': 'apitools/0.2 oauth2l/0.1',
        }
    else:
        return GetDefaultClientInfo()


def _ExpandScopes(scopes):
    scope_prefix = 'https://www.googleapis.com/auth/'
    return [s if s.startswith('https://') else scope_prefix + s
            for s in scopes]


def _PrettyJson(data):
    return json.dumps(data, sort_keys=True, indent=4, separators=(',', ': '))


def _CompactJson(data):
    return json.dumps(data, sort_keys=True, separators=(',', ':'))


def _AsText(text_or_bytes):
    if isinstance(text_or_bytes, bytes):
        return text_or_bytes.decode('utf8')
    return text_or_bytes


def _Format(fmt, credentials):
    """Format credentials according to fmt."""
    if fmt == 'bare':
        return credentials.access_token
    elif fmt == 'header':
        return 'Authorization: Bearer %s' % credentials.access_token
    elif fmt == 'json':
        return _PrettyJson(json.loads(_AsText(credentials.to_json())))
    elif fmt == 'json_compact':
        return _CompactJson(json.loads(_AsText(credentials.to_json())))
    elif fmt == 'pretty':
        format_str = textwrap.dedent('\n'.join([
            'Fetched credentials of type:',
            '  {credentials_type.__module__}.{credentials_type.__name__}',
            'Access token:',
            '  {credentials.access_token}',
        ]))
        return format_str.format(credentials=credentials,
                                 credentials_type=type(credentials))
    raise ValueError('Unknown format: {0}'.format(fmt))

_FORMATS = set(('bare', 'header', 'json', 'json_compact', 'pretty'))


def _GetTokenScopes(access_token):
    """Return the list of valid scopes for the given token as a list."""
    url = _OAUTH2_TOKENINFO_TEMPLATE.format(access_token=access_token)
    response = apitools_base.MakeRequest(
        apitools_base.GetHttp(), apitools_base.Request(url))
    if response.status_code not in [http_client.OK, http_client.BAD_REQUEST]:
        raise apitools_base.HttpError.FromResponse(response)
    if response.status_code == http_client.BAD_REQUEST:
        return []
    return json.loads(_AsText(response.content))['scope'].split(' ')


def _ValidateToken(access_token):
    """Return True iff the provided access token is valid."""
    return bool(_GetTokenScopes(access_token))


def _FetchCredentials(args, client_info=None, credentials_filename=None):
    """Fetch a credential for the given client_info and scopes."""
    client_info = client_info or GetClientInfoFromFlags(args.client_secrets)
    scopes = _ExpandScopes(args.scope)
    if not scopes:
        raise ValueError('No scopes provided')
    credentials_filename = credentials_filename or args.credentials_filename
    # TODO(craigcitro): Remove this logging nonsense once we quiet the
    # spurious logging in oauth2client.
    old_level = logging.getLogger().level
    logging.getLogger().setLevel(logging.ERROR)
    credentials = apitools_base.GetCredentials(
        'oauth2l', scopes, credentials_filename=credentials_filename,
        service_account_json_keyfile=args.service_account_json_keyfile,
        oauth2client_args='', **client_info)
    logging.getLogger().setLevel(old_level)
    if not _ValidateToken(credentials.access_token):
        credentials.refresh(apitools_base.GetHttp())
    return credentials


def _Email(args):
    """Print the email address for this token, if possible."""
    userinfo = apitools_base.GetUserinfo(
        oauth2client.client.AccessTokenCredentials(args.access_token,
                                                   'oauth2l/1.0'))
    user_email = userinfo.get('email')
    if user_email:
        print(user_email)


def _Fetch(args):
    """Fetch a valid access token and display it."""
    credentials = _FetchCredentials(args)
    print(_Format(args.credentials_format.lower(), credentials))


def _Header(args):
    """Fetch an access token and display it formatted as an HTTP header."""
    print(_Format('header', _FetchCredentials(args)))


def _Scopes(args):
    """Print the list of scopes for a valid token."""
    scopes = _GetTokenScopes(args.access_token)
    if not scopes:
        return 1
    for scope in sorted(scopes):
        print(scope)


def _Userinfo(args):
    """Print the userinfo for this token, if possible."""
    userinfo = apitools_base.GetUserinfo(
        oauth2client.client.AccessTokenCredentials(args.access_token,
                                                   'oauth2l/1.0'))
    if args.format == 'json':
        print(_PrettyJson(userinfo))
    else:
        print(_CompactJson(userinfo))


def _Validate(args):
    """Validate an access token. Exits with 0 if valid, 1 otherwise."""
    return 1 - (_ValidateToken(args.access_token))


def _GetParser():

    shared_flags = argparse.ArgumentParser(add_help=False)
    shared_flags.add_argument(
        '--client_secrets',
        default='',
        help=('If specified, use the client ID/secret from the named '
              'file, which should be a client_secrets.json file '
              'downloaded from the Developer Console.'))
    shared_flags.add_argument(
        '--credentials_filename',
        default='',
        help='(optional) Filename for fetching/storing credentials.')
    shared_flags.add_argument(
        '--service_account_json_keyfile',
        default='',
        help=('Filename for a JSON service account key downloaded from '
              'the Google Developer Console.'))

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest='command')

    # email
    email = subparsers.add_parser('email', help=_Email.__doc__,
                                  parents=[shared_flags])
    email.set_defaults(func=_Email)
    email.add_argument(
        'access_token',
        help=('Access token to print associated email address for. Must have '
              'the userinfo.email scope.'))

    # fetch
    fetch = subparsers.add_parser('fetch', help=_Fetch.__doc__,
                                  parents=[shared_flags])
    fetch.set_defaults(func=_Fetch)
    fetch.add_argument(
        '-f', '--credentials_format',
        default='pretty', choices=sorted(_FORMATS),
        help='Output format for token.')
    fetch.add_argument(
        'scope',
        nargs='*',
        help='Scope to fetch. May be provided multiple times.')

    # header
    header = subparsers.add_parser('header', help=_Header.__doc__,
                                   parents=[shared_flags])
    header.set_defaults(func=_Header)
    header.add_argument(
        'scope',
        nargs='*',
        help='Scope to header. May be provided multiple times.')

    # scopes
    scopes = subparsers.add_parser('scopes', help=_Scopes.__doc__,
                                   parents=[shared_flags])
    scopes.set_defaults(func=_Scopes)
    scopes.add_argument(
        'access_token',
        help=('Scopes associated with this token will be printed.'))

    # userinfo
    userinfo = subparsers.add_parser('userinfo', help=_Userinfo.__doc__,
                                     parents=[shared_flags])
    userinfo.set_defaults(func=_Userinfo)
    userinfo.add_argument(
        '-f', '--format',
        default='json', choices=('json', 'json_compact'),
        help='Output format for userinfo.')
    userinfo.add_argument(
        'access_token',
        help=('Access token to print associated email address for. Must have '
              'the userinfo.email scope.'))

    # validate
    validate = subparsers.add_parser('validate', help=_Validate.__doc__,
                                     parents=[shared_flags])
    validate.set_defaults(func=_Validate)
    validate.add_argument(
        'access_token',
        help='Access token to validate.')

    return parser


def main(argv=None):
    argv = argv or sys.argv
    # Invoke the newly created parser.
    args = _GetParser().parse_args(argv[1:])
    try:
        exit_code = args.func(args)
    except BaseException as e:
        print('Error encountered in {0} operation: {1}'.format(
            args.command, e))
        return 1
    return exit_code


if __name__ == '__main__':
    sys.exit(main(sys.argv))
