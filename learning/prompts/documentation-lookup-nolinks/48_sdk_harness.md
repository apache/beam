Prompt:
What is SDK harness in Apache Beam?

Response:
Interoperability between Apache Beam SDKs and runners is a key aspect of Apache Beam. The portability framework introduces well-defined, language-neutral data structures and protocols between the SDK and runner. This interoperability layer, known as the 'portability API', ensures that SDKs and runners can seamlessly work with each other, reducing the interoperability burden for both SDKs and runners to a constant effort.

The portability API consists of a set of smaller contracts that isolate SDKs and runners for job submission, management, and execution. These contracts utilize protocols like `protobuf` and `gRPC` to provide broad language support. Currently, all SDKs support the portability framework.

The SDK harness is a program responsible for executing user code. This program is provided by an SDK and runs separately from the runner. SDK harness initialization relies on the provision and artifact APIs for obtaining staged files, pipeline options, and environment information.

Apache Beam offers configuration options for the SDK harness to cater to diverse cluster setups. These options include:
1. **`environment_type`**: determines where user code is executed. The `environment_config` parameter configures the environment based on the value of `environment_type`:
  * `DOCKER`: executes user code within a container on each worker node. Docker must be installed on worker nodes. You can specify the Docker image URL using the `environment_config` parameter. Prebuilt SDK container images are available with each Apache Beam release and pushed to Docker Hub. You can also build your custom image.
  * `PROCESS`: executes user code through processes that are automatically initiated by the runner on each worker node.
  * `EXTERNAL`: dispatches user code to an external service. Use the `environment_config` parameter to specify the service address, for example, `localhost:50000`.
  * `LOOPBACK`: executes user code within the same process that submitted the pipeline.
2. **`sdk_worker_parallelism`**: determines the number of SDK workers per worker node. The default value is 1, but setting it to 0 enables automatic determination by the runner based on factors like the number of CPU cores on the worker machine.