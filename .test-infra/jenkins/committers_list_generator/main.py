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

import ldap
import logging
import jinja2
import os
import argparse


class ApacheLDAPException(Exception):
    pass


_FILENAME = "Committers.groovy"
_PEOPLE_DN = "ou=people,dc=apache,dc=org"
_BEAM_DN = "cn=beam,ou=project,ou=groups,dc=apache,dc=org"
_GITHUB_USERNAME_ATTR = "githubUsername"
_DEFAULT_LDAP_URIS = "ldaps://ldap-us-ro.apache.org:636 ldaps://ldap-eu-ro.apache.org:636"
_DEFAULT_CERT_PATH = "cert/"

def generate_groovy(output_dir, ldap_uris, cert_path):
    env = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"))
    template = env.get_template(f"{_FILENAME}.template")
    with open(os.path.join(output_dir, _FILENAME), "w") as file:
        file.write(
            template.render(
                github_usernames=get_committers_github_usernames(
                    ldap_uris=ldap_uris,
                    cert_path=cert_path,
                ),
            )
        )


def get_committers_github_usernames(ldap_uris, cert_path):
    connection = None
    try:
        ldap.set_option(ldap.OPT_X_TLS_CACERTFILE, cert_path)
        ldap.set_option(ldap.OPT_X_TLS, ldap.OPT_X_TLS_DEMAND)
        ldap.set_option(ldap.OPT_X_TLS_DEMAND, True)
        ldap.set_option(ldap.OPT_REFERRALS, 0)
        connection = ldap.initialize(ldap_uris)

        people = connection.search_s(
            _PEOPLE_DN,
            ldap.SCOPE_ONELEVEL,
            attrlist=[_GITHUB_USERNAME_ATTR],
        )

        if not people:
            raise ApacheLDAPException(f"LDAP server returned no people: {repr(people)}")

        github_usernames = {
            person_dn: data.get(_GITHUB_USERNAME_ATTR, [])
            for person_dn, data in people
        }

        committers = connection.search_s(
            _BEAM_DN,
            ldap.SCOPE_BASE,
            attrlist=["member"],
        )

        if not committers or "member" not in committers[0][1]:
            raise ApacheLDAPException(f"LDAP server returned no committers: {repr(committers)}")

        committers_github_usernames = [
            github_username.decode()
            for committer_dn in committers[0][1]["member"]
            for github_username in github_usernames[committer_dn.decode()]
        ]
        return committers_github_usernames

    except (ldap.LDAPError, ApacheLDAPException) as e:
        logging.error(f"Could not fetch LDAP data: {e}")
        return []
    finally:
        if connection is not None:
            connection.unbind_s()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generates groovy file containing beam committers' usernames."
    )

    parser.add_argument(
        "-o", "--output-dir",
        help="Path to the directory where the output groovy file will be saved",
        metavar="DIR",
        default=os.getcwd(),
    )

    parser.add_argument(
        "-c", "--cert-path",
        help="Path to the file containing SSL certificate of the LDAP server",
        metavar="FILE",
        default=_DEFAULT_CERT_PATH,
    )

    parser.add_argument(
        "-u", "--ldap_uris",
        help="Whitespace separated list of LDAP servers URIs",
        default=_DEFAULT_LDAP_URIS,
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    generate_groovy(args.output_dir, args.ldap_uris, args.cert_path)
