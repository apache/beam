<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# TensorRT test resources for Beam

This directory contains the Dockerfile required to run Beam pipelines that use TensorRT,
and the script that rebuilds the TensorRT engines those tests load from GCS.

## Container image

To build the image, run `docker build -f tensor_rt.dockerfile -t us.gcr.io/apache-beam-testing/python-postcommit-it/tensor_rt:latest .`

## Rebuilding the test engines

The TensorRT tests load pre-built engines from `gs://apache-beam-ml/models/`:

| Engine | Used by |
| --- | --- |
| `single_tensor_features_engine.trt` | `tensorrt_inference_test.py` |
| `multiple_tensor_features_engine.trt` | `tensorrt_inference_test.py` |
| `ssd_mobilenet_v2_320x320_coco17_tpu-8.trt` | the `tensorRTtests` Dataflow integration test |

**A serialized TensorRT engine is not portable.** It can only be deserialized by the
same TensorRT major version and the same GPU architecture that built it. So these files
must be rebuilt whenever either of the following changes:

* the TensorRT version in `tensor_rt.dockerfile`, or
* the GPU that the `tensorRTtests` task requests in
  `sdks/python/test-suites/dataflow/common.gradle`.

`build_test_engines.py` does that. It rebuilds each engine from the ONNX source already
staged next to it in the same bucket, so no new model sources are needed, and it verifies
each result by loading it back through `TensorRTEngineHandlerNumPy` — the small engines
against the exact values the unit tests assert, and the object detection engine against
the same COCO images the integration test uses.

It needs a GPU, so it cannot run as part of the test suite. Run it in the same container
and on the same GPU type the tests use. As of writing that is
`nvcr.io/nvidia/tensorrt:26.06-py3` on an `nvidia-tesla-t4`. It requires TensorRT 10 or
later, since the TensorRT 8 engines are the ones being replaced.

The host needs a driver new enough for that container (580 or later), plus Docker and
the NVIDIA container toolkit. On a GCE deep learning VM image the toolkit is already
present but Docker may not be:

```
sudo apt-get install -y docker.io
sudo nvidia-ctk runtime configure --runtime=docker && sudo systemctl restart docker
```

Then, from a directory containing `build_test_engines.py`:

```
sudo docker run --rm --gpus all -v "$PWD:/w" -w /w nvcr.io/nvidia/tensorrt:26.06-py3 bash -c "\
    pip install -q --break-system-packages 'apache-beam[gcp]' cuda-python pillow && \
    python3 build_test_engines.py --dest gs://YOUR_BUCKET/models"
```

`--break-system-packages` is required because the container's Python environment is
marked externally managed. Credentials are picked up from the VM's service account, so
no extra authentication step is needed.

The verification step imports the model handler from the installed `apache-beam`, so
that version has to support the TensorRT major version you are building for. To verify
against an unreleased change, copy your working tree's
`apache_beam/ml/inference/tensorrt_inference.py` over the installed one inside the
container before running the script.

Engines are written with a `_trt<major>` suffix, for example
`single_tensor_features_engine_trt11.trt`, so the engines built by earlier TensorRT
versions stay in place and anyone on an older branch is unaffected.

Point `--dest` at a bucket you can write to. Staging the results under
`gs://apache-beam-ml/models/` is a separate, deliberate step for someone with write
access to that bucket; note it has no object versioning, so an overwrite cannot be undone.

Pass `--only <name>` to rebuild a single engine, and `--help` for the full options.
