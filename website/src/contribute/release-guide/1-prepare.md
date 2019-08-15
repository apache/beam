---
layout: section
title: "Beam Release Guide: 01 - Prepare"
section_menu: section-menu/contribute.html
permalink: /contribute/release-guide/prepare/
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

# Prepare for the release

*****

Before your first release, you should perform one-time configuration steps. 
This will set up your security keys for signing the release and access to various release repositories.

To prepare for each release, you should audit the project status in the JIRA issue tracker,
and do necessary bookkeeping. Finally, you should create a release branch from which individual release candidates will be built.

__NOTE__: If you are using [GitHub two-factor authentication](https://help.github.com/articles/securing-your-account-with-two-factor-authentication-2fa/) and haven't configure HTTPS access, 
please follow [the guide](https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/) to configure command line access.



## One-time setup instructions



### Accounts

Please have these credentials ready at hand, you will likely need to enter them multiple times:

 * GPG pass phrase (see the next section);
 * Apache ID and Password;
 * GitHub ID and Password:
   * see the __NOTE__ in the previous section, some scripts might not work correctly if you have 
   a multi-factor authentication enabled and SSH-access configured,
   and might leave your local clone in an inconsistent state. 
   In these cases you might want to use `git@github.com:apache/beam.git` style remote specification 
   everywhere instead of `https://github.com/apache/beam.git` and double check what commands the script runs; 



*****


### GPG Key

You need to have a GPG key to sign the release artifacts. 
Please be aware of the ASF-wide [release signing guidelines](https://www.apache.org/dev/release-signing.html). 
If you don’t have a GPG key associated with your Apache account, please create one according to the guidelines 
([Apache PGP Guide](https://www.apache.org/dev/openpgp.html#generate-key)).

There are 2 ways to configure your GPG key for release, either using release automation script or running all commands manually.


#### Use preparation_before_release.sh to setup GPG

* Script: [preparation_before_release.sh](https://github.com/apache/beam/blob/master/release/src/main/scripts/preparation_before_release.sh)
  * for this and all other scripts you can run all the steps manually if the script fails at some point;
  * even if you rely on the script please get acquainted with it first to understand what it is doing. 
  Sometimes the scripts don't handle the errors correctly and might leave everything in an inconsistent state 
  (e.g. if you fail to enter the GitHub password correctly multiple times); 

* Usage

  ```
  ./beam/release/src/main/scripts/preparation_before_release.sh
  ```
* Th script does the following:
  1. Helps you create a new GPG key if you want.
  1. Configures ```git user.signingkey``` with chosen pubkey.
  1. Adds the chosen public key into [dev KEYS](https://dist.apache.org/repos/dist/dev/beam/KEYS) and [release KEYS](https://dist.apache.org/repos/dist/release/beam/KEYS)
     
     **NOTES**: Only PMC can write into [release repo](https://dist.apache.org/repos/dist/release/beam/).
  1. Starts GPG agents.

#### Submit your GPG public key into a PGP Public Key Server

In order to make yourself have right permission to stage java artifacts in Apache Nexus staging repository, 
please submit your GPG public key into [MIT PGP Public Key Server](http://pgp.mit.edu:11371/).

If MIT doesn't work for you (it probably won't, it's slow, returns 502 a lot, Nexus might error out not being able to find the keys),
use a keyserver at `ubuntu.com` instead: http://keyserver.ubuntu.com/.



*****


### Access to Apache Nexus repository

Configure access to the [Apache Nexus repository](http://repository.apache.org/). It manages the Java artifacts and enables final
deployment of releases to the Maven Central Repository.

1. You log in with your Apache account.
1. Confirm you have appropriate access by finding `org.apache.beam` under `Staging Profiles`.
1. Navigate to your `Profile` (top right dropdown menu of the page).
1. Choose `User Token` from the dropdown, then click `Access User Token`. Copy a snippet of the Maven XML configuration block.
1. Insert this snippet twice into your global Maven `settings.xml` file, typically `${HOME}/.m2/settings.xml`. The end result should look like this, where `TOKEN_NAME` and `TOKEN_PASSWORD` are your secret tokens:

```
    <!-- make sure you have the root `settings node: -->
    <settings>
      <!-- then `servers` -->
      <servers>
        <!-- and then paste the `server` config for each server -->
        <server>
          <id>apache.releases.https</id>
          <username>TOKEN_NAME</username>
          <password>TOKEN_PASSWORD</password>
        </server>
        <server>
          <id>apache.snapshots.https</id>
          <username>TOKEN_NAME</username>
          <password>TOKEN_PASSWORD</password>
        </server>
      </servers>
    </settings>
```

__NOTE__: make sure the XML you end up with matches the structure above.


*****


### Website development setup

Updating the Beam website requires submitting PRs to both the main `apache/beam`
repo and the `apache/beam-site` repo. The first contains reference manuals
generated from SDK code, while the second updates the current release version
number.

You should already have setup a local clone of `apache/beam`. Setting up a clone
of `apache/beam-site` is similar:

  ```
  $ git clone -b release-docs https://github.com/apache/beam-site.git
  $ cd beam-site
  $ git remote add <GitHub_user> git@github.com:<GitHub_user>/beam-site.git
  $ git fetch --all
  $ git checkout -b <my-branch> origin/release-docs
  ```

Further instructions on website development on `apache/beam` is
[here](https://github.com/apache/beam/blob/master/website). Background
information about how the website is updated can be found in [Beam-Site
Automation Reliability](https://s.apache.org/beam-site-automation).


*****


### Register to PyPI

Release manager needs to have an account with PyPI. If you need one, [register at PyPI](https://pypi.python.org/account/register/).
You also need to be a maintainer (or an owner) of the [apache-beam](https://pypi.python.org/pypi/apache-beam) 
package in order to push a new release. Ask on the mailing list for assistance.


*****


### Create a new version in JIRA

When contributors resolve an issue in JIRA, they are tagging it with a release that will contain their changes. 
With the release currently underway, new issues should be resolved against a subsequent future release. 
Therefore, you should create a release item for this subsequent release, as follows:

__Attention__: Only PMC has permission to perform these tasks. If you are not a PMC, please ask for help on dev@ mailing list.

1. In JIRA, navigate to [`Beam > Administration > Versions`](https://issues.apache.org/jira/plugins/servlet/project-config/BEAM/versions).
1. Add a new release. Choose the next minor version number after the version currently underway, select the release cut date (today’s date) as the `Start Date`, and choose `Add`.
1. At the end of the release, go to the same page and mark the recently released version as released. Use the `...` menu and choose `Release`.

*****
<a class="button button--primary" href="{{'/contribute/release-guide/create-branch/'|prepend:site.baseurl}}">Next Step: Create Branch</a>
