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

# Example RunInference API pipelines

This module contains example pipelines that use the Beam RunInference
API. <!---TODO: Add link to full documentation on Beam website when it's published.-->

## Prerequisites

You must have `apache-beam>=2.40.0` installed in order to run these pipelines,
because the `apache_beam.examples.inference` module was added in that release.
```
pip install apache-beam==2.40.0
```

### PyTorch dependencies

The RunInference API supports the PyTorch framework. To use PyTorch locally, first install `torch`.
```
pip install torch==1.11.0
```

For installation of the `torch` dependency on a distributed runner, like Dataflow, refer to these
[instructions](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#pypi-dependencies).

<!---
TODO: Add link to full documentation on Beam website when it's published.

i.e. "See the
[documentation](https://beam.apache.org/documentation/dsls/dataframes/overview/#pre-requisites)
for details."
-->

### Datasets and models for RunInference

The RunInference example pipelines read example data from `gs://apache-beam-ml/`.
You can view the data
[here](https://console.cloud.google.com/storage/browser/apache-beam-ml). You can
also list the example data using the
[gsutil tool](https://cloud.google.com/storage/docs/gsutil#gettingstarted).

```
gsutil ls gs://apache-beam-ml
```

---
## Image classification with ImageNet dataset

[`pytorch_image_classification.py`](./pytorch_image_classification.py) contains
an implementation for a RunInference pipeline that peforms image classification
on the [ImageNet dataset](https://www.image-net.org/) using the MobileNetV2
architecture.

The pipeline reads the images, performs basic preprocessing, passes them to the
PyTorch implementation of RunInference, and then writes the predictions
to a text file in GCS.

### Dataset and model for image classification

The image classification pipeline uses the following data:
<!---
TODO: Add once benchmark test is released
- `gs://apache-beam-ml/testing/inputs/imagenet_validation_inputs.txt`:
  text file containing the GCS paths of the images of all 5000 imagenet validation data
    - gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00000001.JPEG
    - ...
    - gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00050000.JPEG
-->
- `gs://apache-beam-ml/testing/inputs/it_imagenet_validation_inputs.txt`:
  Text file containing the GCS paths of the images of a subset of ImageNet
  validation data. The following example command shows how to view contents of
  the file:

  ```
  $ gsutil cat gs://apache-beam-ml/testing/inputs/it_imagenet_validation_inputs.txt
  gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00000001.JPEG
  ...
  gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00000015.JPEG
  ```

- `gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_*.JPEG`:
  JPEG images for the entire validation dataset.

- `gs://apache-beam-ml/models/torchvision.models.mobilenet_v2.pth`: Path to
  the location of the saved `state_dict` of the pretrained `mobilenet_v2` model
  from the `torchvision.models` subdirectory.

### Running `pytorch_image_classification.py`

To run the image classification pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.pytorch_image_classification \
  --input gs://apache-beam-ml/testing/inputs/it_imagenet_validation_inputs.txt \
  --output predictions.csv \
  --model_state_dict_path gs://apache-beam-ml/models/torchvision.models.mobilenet_v2.pth
```

This writes the output to `predictions.csv` with contents like:
```
gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00005002.JPEG,333
gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00005003.JPEG,711
gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00005004.JPEG,286
...
```

The second item in each line is the integer representing the predicted class of the
image.
