Prompt:
How to customize compute resources for an Apache Beam pipeline?

Response:
Resource hints allow pipeline authors to provide Apache Beam runners with additional information, suggesting desired compute resource requirements. Runners can apply such hints to configure the execution environment for specific transforms or the entire pipeline.

Currently, Apache Beam supports two resource hints:
* `min_ram`: specifies the minimum amount of RAM to allocate to workers, expressed in MB, GB, MiB, or GiB (for example, `min_ram="4GB"`). This value serves as a lower limit when allocating memory to workers.
* `accelerator`: describes a hardware accelerator for processing a transform, such as specifying GPU type, count, and configuration options.

Resource hints can be nested, allowing hints to be specified on sub-transforms (inner transforms) of a composite transform (parent transform). When applying hints, runners follow these rules:
* For `min_ram`, the largest value among a transform and its parent transforms is used.
*Example: if an inner transform sets `min_ram` to 16GB and the parent transform sets it to 8GB, the inner transform's value (16GB) is used for steps within the inner transform, and the parent transform's value (8GB) is used for steps outside the inner transform. If an inner transform sets min_ram to 16GB and the parent transform sets it to 32GB, a hint of 32GB is used for all steps in the entire transform.*
* For `accelerator`, the innermost value takes precedence.
*Example: if an inner transform's accelerator hint differs from a parent transform's, the inner transform's hint is used.*

***Specify Resource Hints for a Pipeline:***

In Apache Beam's Java SDK, you can specify hints via the `resourceHints` pipeline option. Example:

```java
mvn compile exec:java -Dexec.mainClass=com.example.MyPipeline \
    -Dexec.args="... \
                 --resourceHints=min_ram=<N>GB \
                 --resourceHints=accelerator='hint'" \
    -Pdirect-runner
```

In the Python SDK, you can specify hints via the `resource_hints` pipeline option. Example:

```python
python my_pipeline.py \
    ... \
    --resource_hints min_ram=<N>GB \
    --resource_hints accelerator="hint"
```

***Specify Resource Hints for a Transform:***

In Apache Beam’s Java SDK, you can specify resource hints programmatically on pipeline steps (transforms) using the `setResourceHints` method. Example:

```java
pcoll.apply(MyCompositeTransform.of(...)
    .setResourceHints(
        ResourceHints.create()
            .withMinRam("15GB")
            .withAccelerator("type:nvidia-tesla-k80;count:1;install-nvidia-driver")))

pcoll.apply(ParDo.of(new BigMemFn())
    .setResourceHints(
        ResourceHints.create().withMinRam("30GB")))
```

In the Python SDK, you can specify resource hints programmatically on pipeline steps (transforms) using the `PTransforms.with_resource_hints` method. Example:

```python
pcoll | MyPTransform().with_resource_hints(
    min_ram="4GB",
    accelerator="type:nvidia-tesla-k80;count:1;install-nvidia-driver")

pcoll | beam.ParDo(BigMemFn()).with_resource_hints(
    min_ram="30GB")
```

The interpretation and actuation of resource hints can vary between runners.
