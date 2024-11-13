# Apache Beam Subprocess Example

This example demonstrates how to execute external C++ binaries as subprocesses within an Apache Beam pipeline using the `SubProcessKernel`.

## Prerequisites

*   **Google Cloud Project:** A Google Cloud project with billing enabled.
*   **Dataflow API:** Enable the Dataflow API for your project.
*   **C++ compiler:** You'll need a C++ compiler (like g++) to compile the C++ binaries.

## Steps

1. **Create a [Maven Example project](https://beam.apache.org/get-started/quickstart-java/) that builds against the latest Beam release:**

    ```bash
    mvn archetype:generate \
    -DarchetypeGroupId=org.apache.beam \
    -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
    -DarchetypeVersion=2.60.0 \
    -DgroupId=org.example \
    -DartifactId=word-count-beam \
    -Dversion="0.1" \
    -Dpackage=org.apache.beam.examples \
    -DinteractiveMode=false
    ```
    
2. **Build the project:**

    *   Navigate to the root of the repository (`word-count-beam/`):

        ```bash
        cd word-count-beam/
        ```

    *   Build the project using Maven:

        ```bash
        mvn clean install
        ```

3. **Run the pipeline on Dataflow:**

    ```bash
    mvn compile exec:java \
      -Dexec.mainClass=org.apache.beam.examples.subprocess.ExampleEchoPipeline \
      -Dexec.args="--sourcePath=/absolute/path/to/your/subprocess/directory \
                   --workerPath=/absolute/path/to/your/subprocess/directory \
                   --concurrency=5 \
                   --filesToStage=/absolute/path/to/your/subprocess/directory/echo,/absolute/path/to/your/subprocess/directory/echoagain \
                   --runner=DataflowRunner \
                   --project=your-project-id \
                   --region=your-gcp-region \
                   --tempLocation=gs://your-gcs-bucket/temp"
    ```

    *   Replace the placeholders with your actual paths, project ID, region, and Cloud Storage bucket.

## Important notes

* **Dependencies:** Ensure your `pom.xml` includes the Dataflow runner dependency (`beam-runners-google-cloud-dataflow-java`).
* **Authentication:** Authenticate your environment to your Google Cloud project.
* **DirectRunner:** On `DirectRunner`, you will see the error ` Process succeded but no result file was found`, showing that the Process is successful.

## Code overview

*   **`ExampleEchoPipeline.java`:** This Java file defines the Beam pipeline that executes the `Echo` and `EchoAgain` binaries as subprocesses.
*   **`Echo.cc` and `Echoagain.cc`:** These C++ files contain the code for the external binaries. These won't be visible when running the example with the created example project. You will need to compile these (using `g++ Echo.cc -o Echo` and `g++ EchoAgain.cc -o EchoAgain`), and then provide their path via the `sourcePath` and `workerPath` flags as listed above.
*   **`SubProcessKernel.java`:** This class in the Beam Java SDK handles the execution of external binaries and captures their output.
