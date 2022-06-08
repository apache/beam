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

# Example RunInference API Pipelines

This module contains example pipelines that use the Beam RunInference
API. <!---TODO: Add link to full documentation on Beam website when it's published.-->

## Pre-requisites

You must have `apache-beam>=2.40.0` installed in order to run these pipelines,
because the `apache_beam.examples.inference` module was added in that release.
Using the RunInference API also `torch` to be installed. 

To install for a local pipeline, run:
```
pip install apache-beam torch==1.11.0
```

To install for a Dataflow pipeline, refer to these
[instructions](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#pypi-dependencies).
You'll need to add `torch` to a `requirements.txt` file, and then run your
pipeline with the following command-line option:
```
--requirements_file requirements.txt
```

<!---
TODO: Add link to full documentation on Beam website when it's published.

i.e. "See the
[documentation](https://beam.apache.org/documentation/dsls/dataframes/overview/#pre-requisites)
for details."
-->

## Image Classification with ImageNet dataset

[`pytorch_image_classification.py`](./pytorch_image_classification.py) contains
an implementation for a RunInference pipeline thatpeforms image classification
on [ImageNet dataset](https://www.image-net.org/) using the MobileNetV2
architecture.

The pipeline reads the images, performs basic preprocessing, passes them to the
PyTorch implementation of RunInference, and then writes the predictions
to a text file in GCS.

### Data
Data related to RunInference has been staged in
`gs://apache-beam-ml/` for use with these example pipelines:

<!---
Add once benchmark test is released
- `gs://apache-beam-ml/testing/inputs/it_mobilenetv2_imagenet_validation_inputs.txt`:
  text file containing the GCS paths of the images of all 5000 imagenet validation data
    - gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00000001.JPEG
    - ...
    - gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00050000.JPEG
-->
- `gs://apache-beam-ml/testing/inputs/imagenet_validation_inputs.txt/`:
  text file containing the GCS paths of the images of a subset of 15 imagenet
  validation data
    - gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00000001.JPEG
    - ...
    - gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00000015.JPEG

- `gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_*.JPEG`:
  JPEG images for the entire validation dataset.

- `gs://apache-beam-ml/models/imagenet_classification_mobilenet_v2.pt`: Path to
  the location of the saved state_dict of the pretrained mobilenet_v2 model
  from the `torchvision.models` subdirectory.

### Running `pytorch_image_classification.py`

To run the image classification pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.pytorch_image_classification \
  --input gs://apache-beam-ml/testing/inputs/it_mobilenetv2_imagenet_validation_inputs.txt \
  --output predictions.csv \
  --model_state_dict_path gs://apache-beam-ml/models/imagenet_classification_mobilenet_v2.pt
```

This will write the output to the `predictions.csv` with contents like:
```
gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00005002.JPEG,333
gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00005003.JPEG,711
gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00005004.JPEG,286
gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00005005.JPEG,433
gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00005006.JPEG,290
gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00005007.JPEG,890
gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00005008.JPEG,592
gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00005009.JPEG,406
gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00005010.JPEG,996
gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00005011.JPEG,327
gs://apache-beam-ml/datasets/imagenet/raw-data/validation/ILSVRC2012_val_00005012.JPEG,573
```
where the second item in each line is the integer representing the predicted class of the
image.
