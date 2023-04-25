ARG BEAM_VERSION=2.45.0
FROM python:3.10-slim-buster

############
## Docker ##
############
ARG DOCKER_CHANNEL=stable
ARG DOCKER_VERSION=20.10.8

RUN apt-get update && apt-get install -y --no-install-recommends \
  apt-transport-https \
  ca-certificates \
  curl \
  gnupg2 \
  software-properties-common

RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -

RUN add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/debian \
   $(lsb_release -cs) \
   ${DOCKER_CHANNEL}"

RUN apt-get update && apt-get install -y --no-install-recommends docker-ce=5:${DOCKER_VERSION}~3-0~debian-buster && \
  docker -v && \
  dockerd -v

####################
## Docker Compose ##
####################
# ARG DOCKER_COMPOSE_VERSION=1.29.2
# RUN curl -L https://github.com/docker/compose/releases/download/$DOCKER_COMPOSE_VERSION/docker-compose-Linux-x86_64 > /usr/local/bin/docker-compose && \
#   chmod +x /usr/local/bin/docker-compose

#################
## DIND Script ##
#################
ARG DIND_COMMIT=deda3d4933d3c0bd57f2cef672da5d28fc653706
ENV DOCKER_EXTRA_OPTS '--storage-driver=overlay'
RUN curl -fL -o /usr/local/bin/dind "https://raw.githubusercontent.com/moby/moby/${DIND_COMMIT}/hack/dind" && \
	chmod +x /usr/local/bin/dind

ENV VIRTUAL_ENV=/opt/playground/venv
RUN mkdir -p $VIRTUAL_ENV
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

ARG BEAM_VERSION
RUN pip install --upgrade pip
RUN pip install apache-beam==${BEAM_VERSION}
RUN pip install --default-timeout=100 torch
RUN pip install torchvision
RUN pip install pandas
RUN pip install scikit-learn

RUN apt-get update
RUN apt-get install -y wget

RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add -
RUN add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/
RUN apt-get update
RUN apt-get install -y adoptopenjdk-11-hotspot

# RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add -

# RUN add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/

# RUN apt-get update && apt-get install adoptopenjdk-8-hotspot

# RUN apt update

# RUN apt install software-properties-common

# RUN apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main'

# RUN apt install openjdk-8-jre

# RUN apt-get update && \
# apt-get install -y --no-install-recommends \
#         openjdk-8-jre
        # openjdk-11-jre

# Prints installed java version, just for checking
RUN java -version

#TODO use BUILD_ARG
ARG BEAM_VERSION
ARG REPO_URL=https://repo1.maven.org/maven2
ARG SQL_EXPANSION_SERVICE_JAR=${REPO_URL}/org/apache/beam/beam-sdks-java-extensions-sql-expansion-service/${BEAM_VERSION}/beam-sdks-java-extensions-sql-expansion-service-${BEAM_VERSION}.jar
RUN curl -O -L $SQL_EXPANSION_SERVICE_JAR

################
## Pre-pulled images ##
################
COPY ./images/* /images/

# Install mitmpoxy
# RUN mkdir /opt/mitmproxy &&\
#     cd /opt/mitmproxy &&\
#     curl https://snapshots.mitmproxy.org/7.0.4/mitmproxy-7.0.4-linux.tar.gz &&\
#     tar -zxvf mitmproxy-7.0.4-linux.tar.gz &&\
#     mkdir /usr/local/share/ca-certificates/extra
# COPY allow_list_proxy.py /opt/mitmproxy/
# COPY allow_list.py /opt/mitmproxy/
# ENV HTTP_PROXY="http://127.0.0.1:8081"
# ENV HTTPS_PROXY="http://127.0.0.1:8081"

################
## Entrypoint ##
################
COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh


VOLUME /var/lib/docker
EXPOSE 2375

#TODO FIND A WAY TO USE ARGS HERE!
EXPOSE 9091
EXPOSE 9092
EXPOSE 9093
EXPOSE 9094

# # Create a user group `appgroup` and a user `appuser`
# RUN groupadd --gid 20000 appgroup \
#   && useradd --uid 20000 --gid appgroup --shell /bin/bash --create-home appuser

# # Chown all required files to the `appuser`.
# RUN chmod 777 /usr/local/lib/python3.10/site-packages/certifi/cacert.pem \
#   && chmod -R 777 /usr/local/share/ca-certificates/extra/ && chmod -R 777 /etc/ssl/certs && chmod +x /entrypoint.sh

# # Switch to appuser
# USER appuser

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]