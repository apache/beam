These instructions describe how to install [Prism](../../cmd/prism) on [Kubernetes](https://kubernetes.io).

# Pre-requisites

- [gcloud](https://cloud.google.com/sdk/docs/install)
- https://ko.build
- https://docker.io (See [Docker alternative](#docker-alternative))
- Kubernetes cluster: [minikube](https://minikube.sigs.k8s.io/) or
[Google Kubernetes Engine](../../../../.test-infra/terraform/google-cloud-platform/google-kubernetes-engine)
- https://k9scli.io/ (Optional but highly recommended)

## Docker alternative

An alternative to installing Docker directly on your system is to use https://lima-vm.io/:

```
limactl create --name=default template://docker
```

Follow the instructions that it provides after creating the virtual machine such as creating and setting a 
new docker context.

# 2. Setup/connect to cluster

## Local

Install [minikube](https://minikube.sigs.k8s.io/) and run:

```
minikube start
eval $(minikube -p minikube docker-env)
```

Also set `GOHOSTARCH` to your system's architecture:

```
export GOHOSTARCH=$(arch)
```

## Google Kubernetes Engine

See
https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#default_cluster_kubectl.

# 3. Build and deploy on Kubernetes using ko

[ko](https://ko.build) allows a single command to build and deploy on Kubernetes.

1. Create a prism namespace
    ```
    kubectl create namespace prism
    ```
2. Navigate to the go.mod directory
    ```
    cd sdks
    ```
3. Ko apply
    ```
    ko apply -f ./go/container/prism -L --platform=$(arch)
    ```

# 4. Verify deployment 

Verify 'service', via `kubectl get svc --namespace=prism`:

```
NAME      TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)             AGE
service   ClusterIP   10.110.255.66   <none>        8073/TCP,8074/TCP   103s
```

View logs, via: `kubectl logs deployment/server -c server --namespace=prism`:

```
[2025-01-11T05:26:34.118176788Z]  INFO  Serving JobManagement
* endpoint: localhost:8073

[2025-01-11T05:26:34.11887329Z]  INFO  Serving WebUI
* endpoint: http://localhost:8074
```

# 5. Port forward service

Run the following command to port forward the job and web service to your localhost.

```
kubectl port-forward --namespace=prism svc/service 8073:8073 8074:8074
```

Open http://localhost:8074 for the web UI.

# 6. Launch a pipeline

```
go run ./go/examples/wordcount/wordcount.go \
   --runner=universal \
   --endpoint=localhost:8073 \
   --environment_type=EXTERNAL \
   --environment_config=localhost:8073 \
   --output=/tmp/wc
```