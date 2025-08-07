# VLLM Gemma 2b Batch Performance on Tesla T4

**Model**: google/gemma-2b-it
**Accelerator**: NVIDIA Tesla T4 GPU 
**Host**: 3 × n1-standard-8 (8 vCPUs, 30 GB RAM)

The following graphs show various metrics when running VLLM Gemma 2b Batch Performance on Tesla T4 pipeline.
See the [glossary](/performance/glossary) for definitions.

Full pipeline implementation is available [here](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/vllm_gemma_batch.py).

## What is the estimated cost to run the pipeline?

{{< performance_looks io="vllmgemmabatchtesla" read_or_write="write" section="cost" >}}

## How has various metrics changed when running the pipeline for different Beam SDK versions?

{{< performance_looks io="vllmgemmabatchtesla" read_or_write="write" section="version" >}}

## How has various metrics changed over time when running the pipeline?

{{< performance_looks io="vllmgemmabatchtesla" read_or_write="write" section="date" >}}