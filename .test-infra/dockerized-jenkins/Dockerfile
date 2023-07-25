###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
###############################################################################

FROM jenkins/jenkins:lts

# Jenkins image specifies /var/jenkins_home as volume.
# For matter of copying container, we want to avoid it.
# We have to utilize different folder because docker doesn't
# provide "UNVOLUME" functionality.
ENV JENKINS_HOME=/var/jenkins_real_home/

# Pre-install plugins specified in plugins.txt
COPY --chown=root:jenkins plugins.txt /usr/share/jenkins/ref/plugins.txt
RUN jenkins-plugin-cli -f /usr/share/jenkins/ref/plugins.txt

# Copy default configuration and sample seed job.
COPY --chown=jenkins basic-security.groovy /usr/share/jenkins/ref/init.groovy.d/
COPY --chown=jenkins javaposse.jobdsl.plugin.GlobalJobDslSecurityConfiguration.xml /var/jenkins_real_home/
COPY --chown=jenkins config.xml /var/jenkins_real_home/
COPY --chown=jenkins seedjobconfig.xml /var/jenkins_real_home/jobs/sample_seed_job/config.xml

# Disable setup wizard
RUN echo $JENKINS_VERSION > /usr/share/jenkins/ref/jenkins.install.UpgradeWizard.state
RUN echo $JENKINS_VERSION > /usr/share/jenkins/ref/jenkins.install.InstallUtil.lastExecVersion

