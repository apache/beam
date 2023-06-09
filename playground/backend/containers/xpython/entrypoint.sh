#!/bin/bash
# set -e

# echo "==> Launching the Docker daemon..."
# CMD=$*
# if [ "$CMD" == '' ];then
#   dind dockerd $DOCKER_EXTRA_OPTS
#   check_docker
# else
#   dind dockerd $DOCKER_EXTRA_OPTS &
#   while(! docker info > /dev/null 2>&1); do
#       echo "==> Waiting for the Docker daemon to come online..."
#       sleep 1
#   done
#   echo "==> Docker Daemon is up and running!"
#   echo "==> Running CMD $CMD!"

  
#   java -jar beam-sdks-java-extensions-sql-expansion-service-2.43.0.jar 9092 & \
#   python -m apache_beam.runners.portability.local_job_service_main -p 9091 && fg

#   exec "$CMD"
# fi

set -e

echo "==> Launching the Docker daemon..."

dind dockerd --iptables=false &
while(! docker info > /dev/null 2>&1); do
    echo "==> Waiting for the Docker daemon to come online...???"
    sleep 1
done

echo "==> Docker Daemon is up and running!"
echo "==> Loading pre-pulled docker images!"
# Import pre-installed images
for file in /images/*.tar; do
echo "Loading $file"
  docker load <$file
done
rm -f -r images

docker images

# nohup /opt/mitmproxy/mitmdump -s /opt/mitmproxy/allow_list_proxy.py -p 8081 &
# while [ ! -f /home/appuser/.mitmproxy/mitmproxy-ca.pem ] ;
# do
#       sleep 2
# done
# openssl x509 -in /home/appuser/.mitmproxy/mitmproxy-ca.pem -inform PEM -out /home/appuser/.mitmproxy/mitmproxy-ca.crt
# cp /home/appuser/.mitmproxy/mitmproxy-ca.crt /usr/local/share/ca-certificates/extra/
# update-ca-certificates

python -m apache_beam.runners.portability.local_job_service_main -p 9091 &> jjs.log  & \
java -jar beam-sdks-java-extensions-sql-expansion-service-2.45.0.jar 9092 &> jes.log  & \
python -m apache_beam.runners.portability.local_job_service_main -p 9093 &> pjs.log  & \
python -m apache_beam.runners.portability.expansion_service_main -p 9094 --fully_qualified_name_glob "*" &> pes.log  && fg

while true; do sleep 1; done
