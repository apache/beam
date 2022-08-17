# Version. Can change in build progress
ARG GCLOUD_SDK_VERSION=397.0.0-emulators

# Use google cloud sdk
FROM google/cloud-sdk:$GCLOUD_SDK_VERSION

# Volume to persist Datastore data
VOLUME /opt/data

# RUN mkdir -p /opt/data/WEB-INF
COPY index.yaml /opt/data/WEB-INF/index.yaml
COPY start-datastore.sh .

EXPOSE 8081

ENTRYPOINT ["./start-datastore.sh"]
