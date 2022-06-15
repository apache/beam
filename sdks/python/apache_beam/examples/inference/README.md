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

If you are using pretrained models from Pytorch's `torchvision.models` [subpackage](https://pytorch.org/vision/0.12/models.html#models-and-pre-trained-weights), you may also need to install `torchvision`.
```
pip install torchvision
```

If you are using pretrained models from Hugging Face's `transformers` [package](https://huggingface.co/docs/transformers/index), you may also need to install `transformers`.
```
pip install transformers
```

For installation of the `torch` dependency on a distributed runner, like Dataflow, refer to these [instructions](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#pypi-dependencies).

<!---
TODO: Add link to full documentation on Beam website when it's published.

i.e. "See the
[documentation](https://beam.apache.org/documentation/dsls/dataframes/overview/#pre-requisites)
for details."
-->

---
## Image classification

[`pytorch_image_classification.py`](./pytorch_image_classification.py) contains an implementation for a RunInference pipeline that performs image classification using the mobilenet_v2 architecture.

The pipeline reads the images, performs basic preprocessing, passes them to the PyTorch implementation of RunInference, and then writes the predictions to a text file.

### Dataset and model for image classification

You will need to create or download images, and place them into your `IMAGES_DIR` directory. One popular dataset is from [ImageNet](https://www.image-net.org/). Please follow their instructions to download the images.
- **Required**: A path to a file called `IMAGE_FILE_NAMES` that contains the absolute paths of each of the images in `IMAGES_DIR` on which you want to run image segmentation. Paths can be different types of URIs such as your local file system, a AWS S3 bucket or GCP Cloud Storage bucket. For example:
```
/absolute/path/to/image1.jpg
/absolute/path/to/image2.jpg
```
- **Required**: A path to a file called `MODEL_STATE_DICT` that contains the saved parameters of the maskrcnn_resnet50_fpn model. You will need to download the [mobilenet_v2](https://pytorch.org/vision/stable/_modules/torchvision/models/mobilenetv2.html) model from Pytorch's repository of pretrained models. Note that this requires `torchvision` library.
```
import torch
from torchvision.models.detection import mobilenet_v2
model = mobilenet_v2(pretrained=True)
torch.save(model.state_dict(), 'mobilenet_v2.pth')
```
- **Required**: A path to a file called `OUTPUT`, to which the pipeline will write the predictions.
- **Optional**: `IMAGES_DIR`, which is the path to the directory where images are stored. Not required if image names in the input file `IMAGE_FILE_NAMES` have absolute paths.

### Running `pytorch_image_classification.py`

To run the image classification pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.pytorch_image_classification \
  --input IMAGE_FILE_NAMES \
  --images_dir IMAGES_DIR \
  --output OUTPUT \
  --model_state_dict_path MODEL_STATE_DICT
```
For example:
```sh
python -m apache_beam.examples.inference.pytorch_image_classification \
  --input image_file_names.txt \
  --output predictions.csv \
  --model_state_dict_path maskrcnn_resnet50_fpn.pth
```
This writes the output to the `predictions.csv` with contents like:
```
/absolute/path/to/image1.jpg;1
/absolute/path/to/image2.jpg;333
...
```
---
## Image segmentation

[`pytorch_image_segmentation.py`](./pytorch_image_segmentation.py) contains an implementation for a RunInference pipeline that performs image segementation using the maskrcnn_resnet50_fpn architecture.

The pipeline reads images, performs basic preprocessing, passes them to the PyTorch implementation of RunInference, and then writes the predictions to a text file.

### Dataset and model for image segmentation
You will need to create or download images, and place them into your `IMAGES_DIR` directory. Another popular dataset is from [Coco](https://cocodataset.org/#home). Please follow their instructions to download the images.
- **Required**: A path to a file called `IMAGE_FILE_NAMES` that contains the absolute paths of each of the images in `IMAGES_DIR` on which you want to run image segmentation. Paths can be different types of URIs such as your local file system, a AWS S3 bucket or GCP Cloud Storage bucket. For example:
```
/absolute/path/to/image1.jpg
/absolute/path/to/image2.jpg
```
- **Required**: A path to a file called `MODEL_STATE_DICT` that contains the saved parameters of the maskrcnn_resnet50_fpn model. You will need to download the [maskrcnn_resnet50_fpn](https://pytorch.org/vision/0.12/models.html#id70)
model from Pytorch's repository of pretrained models. Note that this requires `torchvision` library.
```
import torch
from torchvision.models.detection import maskrcnn_resnet50_fpn
model = maskrcnn_resnet50_fpn(pretrained=True)
torch.save(model.state_dict(), 'maskrcnn_resnet50_fpn.pth')
```
- **Required**: A path to a file called `OUTPUT`, to which the pipeline will write the predictions.
- **Optional**: `IMAGES_DIR`, which is the path to the directory where images are stored. Not required if image names in the input file `IMAGE_FILE_NAMES` have absolute paths.
### Running `pytorch_image_segmentation.py`

To run the image segmentation pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.pytorch_image_segmentation \
  --input IMAGE_FILE_NAMES \
  --images_dir IMAGES_DIR \
  --output OUTPUT \
  --model_state_dict_path MODEL_STATE_DICT
```
For example:
```sh
python -m apache_beam.examples.inference.pytorch_image_segmentation \
  --input image_file_names.txt \
  --output predictions.csv \
  --model_state_dict_path maskrcnn_resnet50_fpn.pth
```
This writes the output to the `predictions.csv` with contents like:
```
/absolute/path/to/image1.jpg;['parking meter', 'bottle', 'person', 'traffic light', 'traffic light', 'traffic light']
/absolute/path/to/image2.jpg;['bottle', 'person', 'person']
...
```
Each line has data separated by a semicolon ";". The first item is the file name. The second item is a list of predicted instances.

---
## Language modeling

[`pytorch_language_modeling.py`](./pytorch_language_modeling.py) contains an implementation for a RunInference pipeline that performs masked language modeling (i.e. decoding a masked token in a sentence) using the BertForMaskedLM architecture from Hugging Face.

The pipeline reads sentences, performs basic preprocessing to convert the last word into a `[MASK]` token, passes the masked sentence to the PyTorch implementation of RunInference, and then writes the predictions to a text file.

### Dataset and model for language modeling

- **Required**: A path to a file called `MODEL_STATE_DICT` that contains the saved parameters of the BertForMaskedLM model. You will need to download the [BertForMaskedLM](https://huggingface.co/docs/transformers/model_doc/bert#transformers.BertForMaskedLM) model from Hugging Face's repository of pretrained models. Make sure you have installed `transformers` too.
```
import torch
from transformers import BertForMaskedLM
model = BertForMaskedLM.from_pretrained('bert-base-uncased', return_dict=True)
torch.save(model.state_dict(), 'BertForMaskedLM.pth')
```
- **Required**: A path to a file called `OUTPUT`, to which the pipeline will write the predictions.
- **Optional**: A path to a file called `SENTENCES` that contains sentences to feed into the model. It should look something like this:
```
The capital of France is Paris .
He looked up and saw the sun and stars .
...
```
### Running `pytorch_language_modeling.py`

To run the language modeling pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.pytorch_language_modeling \
  --input SENTENCES \
  --output OUTPUT \
  --model_state_dict_path MODEL_STATE_DICT
```
For example:
```sh
python -m apache_beam.examples.inference.pytorch_language_modeling \
  --input sentences.txt \
  --output predictions.csv \
  --model_state_dict_path BertForMaskedLM.pth
```
If you don't provide a sentences file, it will run the pipeline with some
example sentences.
```sh
python -m apache_beam.examples.inference.pytorch_language_modeling \
  --output predictions.csv \
  --model_state_dict_path BertForMaskedLM.pth
```

This writes the output to the `predictions.csv` with contents like:
```
The capital of France is Paris .;paris
He looked up and saw the sun and stars .;moon
...
```
Each line has data separated by a semicolon ";".
The first item is the sentence with the last word masked. The second item
is the word that the model predicts for the mask.

---
## MNITST digit classification
[`sklearn_mnist_classification.py`](./sklearn_mnist_classification.py) contains an implementation for a RunInference pipeline that performs image classification on handwritten digits from the [MNIST](https://en.wikipedia.org/wiki/MNIST_database) database.

The pipeline reads rows of pixels corresponding to a digit, performs basic preprocessing, passes the pixels to the Scikit-learn implementation of RunInference, and then writes the predictions to a text file.

### Dataset and model for language modeling
- **Required**: A path to a file called `INPUT` that contains label and pixels to feed into the model. Each row should have elements that are comma-separated. The first element is the label. All subsuequent elements would be pixel values. It should look something like this:
```
1,0,0,0...
0,0,0,0...
1,0,0,0...
4,0,0,0...
...
```
- **Required**: A path to a file called `OUTPUT`, to which the pipeline will write the predictions.
- **Required**: A path to a file called `MODEL_PATH` that contains the pickled file of a scikit-learn model trained on MNIST data. Please refer to this scikit-learn [documentation](https://scikit-learn.org/stable/model_persistence.html) on how to serialize models.


### Running `sklearn_mnist_classification.py`

To run the MNIST classification pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.sklearn_mnist_classification.py \
  --input_file INPUT \
  --output OUTPUT \
  --model_path MODEL_PATH
```
For example:
```sh
python -m apache_beam.examples.inference.sklearn_mnist_classification.py \
  --input_file mnist_data.csv \
  --output predictions.txt \
  --model_path mnist_model_svm.pickle
```

This writes the output to the `predictions.txt` with contents like:
```
1,1
4,9
7,1
0,0
...
```
Each line has data separated by a comma ",". The first item is the actual label of the digit. The second item is the predicted label of the digit.