---
title: "Resource hints"
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Resource hints

Resource hints let pipeline authors provide information to a runner about compute resource requirements. You can use resource hints to define requirements for specific transforms or for an entire pipeline. The runner is responsible for interpreting resource hints, and runners can ignore unsupported hints.

Resource hints can be nested. For example, resource hints can be specified on subtransforms of a composite transform, and that composite transform can also have resource hints applied. By default, the innermost hint takes precedence. However, hints can define custom reconciliation behavior. For example,  `min_ram` takes the maximum value for all `min_ram` values set on a given step in the pipeline.

{{< language-switcher java py >}}

## Available hints

Currently, Beam supports the following resource hints:

* `min_ram="numberXB"`: The minimum amount of RAM to allocate to workers. Beam can parse various byte units, including MB, GB, MiB, and GiB (for example, `min_ram="4GB"`). This hint is intended to provide advisory minimal memory requirements for processing a transform.
* `accelerator="hint"`: This hint is intended to describe a hardware accelerator to use for processing a transform. For example, the following is valid accelerator syntax for the Dataflow runner: `accelerator="type:<type>;count:<n>;<options>"`

The interpretaton and actuation of resource hints can vary between runners. For an example implementation, see the [Dataflow resource hints](https://cloud.google.com/dataflow/docs/guides/right-fitting#available_resource_hints).

## Specifying resource hints for a pipeline

To specify resource hints for an entire pipeline, you can use pipeline options. The following command shows the basic syntax.

{{< highlight java >}}
mvn compile exec:java -Dexec.mainClass=com.example.MyPipeline \
    -Dexec.args="... \
                 --resourceHints=min_ram=<N>GB \
                 --resourceHints=accelerator='hint'" \
    -Pdirect-runner
{{< /highlight >}}
{{< highlight py >}}
python my_pipeline.py \
    ... \
    --resource_hints min_ram=<N>GB \
    --resource_hints accelerator="hint"
{{< /highlight >}}

## Specifying resource hints for a transform

{{< paragraph class="language-java" >}}
You can set resource hints programmatically on pipeline transforms using [setResourceHints](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/PTransform.html#setResourceHints-org.apache.beam.sdk.transforms.resourcehints.ResourceHints-).
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
You can set resource hints programmatically on pipeline transforms using [PTransforms.with_resource_hints](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.ptransform.html#apache_beam.transforms.ptransform.PTransform.with_resource_hints) (also see [ResourceHint](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/resources.py#L51)).
{{< /paragraph >}}

{{< highlight java >}}
pcoll.apply(MyCompositeTransform.of(...)
    .setResourceHints(
        ResourceHints.create()
            .withMinRam("15GB")
            .withAccelerator("type:nvidia-tesla-k80;count:1;install-nvidia-driver")))

pcoll.apply(ParDo.of(new BigMemFn())
    .setResourceHints(
        ResourceHints.create().withMinRam("30GB")))
{{< /highlight >}}
{{< highlight py >}}
pcoll | MyPTransform().with_resource_hints(
    min_ram="4GB",
    accelerator="type:nvidia-tesla-k80;count:1;install-nvidia-driver")

pcoll | beam.ParDo(BigMemFn()).with_resource_hints(
    min_ram="30GB")
{{< /highlight >}}
