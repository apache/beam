Prompt:
What is SDK Harness in Apache Beam?

Response:
Interoperability between Apache Beam SDKs and runners is a key aspect of Apache Beam. The portability framework introduces well-defined, language-neutral data structures and protocols between the SDK and runner. This interop layer – called the `Portability API` – ensures that SDKs and runners can work with each other uniformly, reducing the interoperability burden for both SDKs and runners to a constant effort.

The portability API consists of a set of smaller contracts that isolate SDKs and runners for job submission, management and execution. These contracts use `protobuf`s and `gRPC` for broad language support. All SDKs currently support the portability framework.

The SDK harness is a SDK-provided program responsible for executing user code and is run separately from the runner. SDK harness initialization relies on the Provision and `Artifact API`s for obtaining staged files, pipeline options and environment information.

Apache Beam allows configuration of the SDK harness to accommodate varying cluster setups:

* **environment_type**: determines where user code will be executed:
  * **DOCKER**: User code is executed within a container started on each worker node. This requires docker to be installed on worker nodes (default). Use `environment_config`  to specify the Docker image URL. Official Docker images are used by default. Alternatively, you can build your own image. Prebuilt SDK container images are released per supported language during Beam releases and pushed to Docker Hub.
  * **PROCESS**: User code is executed by processes that are automatically started by the runner on each worker node.
  * **EXTERNAL**: User code will be dispatched to an external service. Use `environment_config` to specify the address for the external service, e.g. `localhost:50000`.
  * **LOOPBACK**: User code is executed within the same process that submitted the pipeline.

* **sdk_worker_parallelism**:  sets the number of SDK workers that run on each worker node. The default is 1. If 0, the value is automatically set by the runner by looking at different parameters, such as the number of CPU cores on the worker machine.