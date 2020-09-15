---
title: "Beam Security"
aliases: /security/CVE-2020-1929/
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Reporting Security Issues

Apache Beam uses the standard process outlined by the [Apache Security
Team](https://www.apache.org/security/) for reporting vulnerabilities. Note
that vulnerabilities should not be publicly disclosed until the project has
responded.

To report a possible security vulnerability, please email
`security@apache.org` and `pmc@beam.apache.org`. This is a non-public list
that will reach the Beam PMC.

# Known Security Issues

## CVE-2020-1929

[CVE-2020-1929] Apache Beam MongoDB IO connector disables certificate trust verification

Severity: Major
Vendor: The Apache Software Foundation

Versions Affected:
Apache Beam 2.10.0 to 2.16.0

Description:
The Apache Beam MongoDB connector in versions 2.10.0 to 2.16.0 has an option to
disable SSL trust verification. However this configuration is not respected and
the certificate verification disables trust verification in every case. This
exclusion also gets registered globally which disables trust checking for any
code running in the same JVM.

Mitigation:
Users of the affected versions should apply one of the following mitigations:
- Upgrade to Apache Beam 2.17.0 or later

Acknowledgements:
This issue was reported (and fixed) by Colm Ó hÉigeartaigh.
