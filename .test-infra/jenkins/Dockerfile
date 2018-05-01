
FROM python:2.7-alpine
MAINTAINER dataflow-engprod dataflow-engprod@google.com

RUN pip install virtualenv
RUN apk add --update --no-cache python-dev build-base bash openssl gcc
RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-189.0.0-linux-x86_64.tar.gz -O gcloud.tar.gz
RUN tar xf gcloud.tar.gz
RUN ./google-cloud-sdk/install.sh --quiet
ENV PATH="/google-cloud-sdk/bin:/usr/sbin:${PATH}"
RUN gcloud components update --quiet || echo 'gcloud components update failed'
RUN echo $PATH && gcloud --version

ENV CANDIDATE_URL=https://dist.apache.org/repos/dist/dev/beam/2.4.0/ \
    BEAM_PYTHON_SDK=apache-beam-2.4.0-python.zip
RUN wget $CANDIDATE_URL$BEAM_PYTHON_SDK
RUN virtualenv temp_virtualenv && . temp_virtualenv/bin/activate
RUN pip install $BEAM_PYTHON_SDK[gcp]

#COPY ./docker-entrypoint.sh docker-entrypoint.sh
#ENTRYPOINT ['docker-entrypoint.sh']
