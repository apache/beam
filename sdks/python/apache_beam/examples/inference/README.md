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

Some examples are also used in [our benchmarks](http://s.apache.org/beam-community-metrics/d/ZpS8Uf44z/python-ml-runinference-benchmarks?orgId=1).

## Prerequisites

You must have the latest (possibly unreleased) `apache-beam` or greater installed from the Beam repo in order to run these pipelines,
because some examples rely on the latest features that are actively in development. To install Beam, run the following from the `sdks/python` directory:
```
pip install -e .[gcp]
```

### Tensorflow dependencies

The following installation requirement is for the Tensorflow model handler examples.

The RunInference API supports the Tensorflow framework. To use Tensorflow locally, first install `tensorflow`.
```
pip install tensorflow==2.12.0
```


### PyTorch dependencies

The following installation requirements are for the files used in these examples.

The RunInference API supports the PyTorch framework. To use PyTorch locally, first install `torch`.
```
pip install torch==1.10.0
```

If you are using pretrained models from Pytorch's `torchvision.models` [subpackage](https://pytorch.org/vision/0.12/models.html#models-and-pre-trained-weights), you also need to install `torchvision`.
```
pip install torchvision
```

If you are using pretrained models from Hugging Face's `transformers` [package](https://huggingface.co/docs/transformers/index), you also need to install `transformers`.
```
pip install transformers
```

For installation of the `torch` dependency on a distributed runner such as Dataflow, refer to the
[PyPI dependency instructions](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#pypi-dependencies).


### Transformers dependencies

The following installation requirement is for the Hugging Face model handler examples.

The RunInference API supports loading models from the Hugging Face Hub. To use it, first install `transformers`.
```
pip install transformers==4.30.0
```
Additional dependicies for PyTorch and TensorFlow may need to be installed separately:
```
pip install tensorflow==2.12.0
pip install torch==1.10.0
```


### TensorRT dependencies

The RunInference API supports TensorRT SDK for high-performance deep learning inference with NVIDIA GPUs.
To use TensorRT locally, we suggest an environment with TensorRT >= 8.0.1. Install TensorRT as per the
[TensorRT Install Guide](https://docs.nvidia.com/deeplearning/tensorrt/install-guide/index.html). You
will need to make sure the Python bindings for TensorRT are also installed correctly, these are available by installing the python3-libnvinfer and python3-libnvinfer-dev packages on your TensorRT download.

If you would like to use Docker, you can use an NGC image like:
```
docker pull nvcr.io/nvidia/tensorrt:22.04-py3
```
as an existing container base to [build custom Apache Beam container](https://beam.apache.org/documentation/runtime/environments/#modify-existing-base-image).


### ONNX dependencies

The RunInference API supports ONNX runtime for accelerated inference.
To use ONNX, we suggest installing the following dependencies:
```
pip install onnxruntime==1.13.1
```
The onnxruntime dependency is sufficient if you already have a model in onnx format. This library also supports conversion from PyTorch models to ONNX.
If you need to convert TensorFlow models into ONNX, please install:
```
pip install tf2onnx==1.13.0
```
If you need to convert sklearn models into ONNX, please install:
```
pip install skl2onnx
```

### Additional resources
For more information, see the
[About Beam ML](/documentation/ml/about-ml) and the
[RunInference transform](/documentation/transforms/python/elementwise/runinference) documentation.

---
## Image classification

[`pytorch_image_classification.py`](./pytorch_image_classification.py) contains an implementation for a RunInference pipeline that performs image classification using the `mobilenet_v2` architecture.

The pipeline reads the images, performs basic preprocessing, passes the images to the PyTorch implementation of RunInference, and then writes the predictions to a text file.

### Dataset and model for image classification

To use this transform, you need a dataset and model for image classification.

1. Create a directory named `IMAGES_DIR`. Create or download images and put them in this directory. The directory is not required if image names in the input file `IMAGE_FILE_NAMES.txt` you create in step 2 have absolute paths.
One popular dataset is from [ImageNet](https://www.image-net.org/). Follow their instructions to download the images.
2. Create a file named `IMAGE_FILE_NAMES.txt` that contains the absolute paths of each of the images in `IMAGES_DIR` that you want to use to run image classification. The path to the file can be different types of URIs such as your local file system, an AWS S3 bucket, or a GCP Cloud Storage bucket. For example:
```
/absolute/path/to/image1.jpg
/absolute/path/to/image2.jpg
```
3. Download the [mobilenet_v2](https://pytorch.org/vision/stable/_modules/torchvision/models/mobilenetv2.html) model from Pytorch's repository of pretrained models. This model requires the torchvision library. To download this model, run the following commands from a Python shell:
```
import torch
from torchvision.models import mobilenet_v2
model = mobilenet_v2(pretrained=True)
torch.save(model.state_dict(), 'mobilenet_v2.pth') # You can replace mobilenet_v2.pth with your preferred file name for your model state dictionary.
```

### Running `pytorch_image_classification.py`

To run the image classification pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.pytorch_image_classification \
  --input IMAGE_FILE_NAMES \
  --images_dir IMAGES_DIR \
  --output OUTPUT \
  --model_state_dict_path MODEL_STATE_DICT
```
`images_dir` is only needed if your `IMAGE_FILE_NAMES.txt` file contains relative paths (they will be relative from `IMAGES_DIR`).

For example, if you've followed the naming conventions recommended above:
```sh
python -m apache_beam.examples.inference.pytorch_image_classification \
  --input IMAGE_FILE_NAMES.txt \
  --output predictions.csv \
  --model_state_dict_path mobilenet_v2.pth
```
This writes the output to the `predictions.csv` with contents like:
```
/absolute/path/to/image1.jpg;1
/absolute/path/to/image2.jpg;333
...
```

Each image path is paired with a value representing the Imagenet class that returned the highest confidence score out of Imagenet's 1000 classes.

---
## Image segmentation

[`pytorch_image_segmentation.py`](./pytorch_image_segmentation.py) contains an implementation for a RunInference pipeline that performs image segementation using the `maskrcnn_resnet50_fpn` architecture.

The pipeline reads images, performs basic preprocessing, passes the images to the PyTorch implementation of RunInference, and then writes predictions to a text file.

### Dataset and model for image segmentation

To use this transform, you need a dataset and model for image segmentation.

1. Create a directory named `IMAGES_DIR`. Create or download images and put them in this directory. The directory is not required if image names in the input file `IMAGE_FILE_NAMES.txt` you create in step 2 have absolute paths.
A popular dataset is from [Coco](https://cocodataset.org/#home). Follow their instructions to download the images.
2. Create a file named `IMAGE_FILE_NAMES.txt` that contains the absolute paths of each of the images in `IMAGES_DIR` that you want to use to run image segmentation. The path to the file can be different types of URIs such as your local file system, an AWS S3 bucket, or a GCP Cloud Storage bucket. For example:
```
/absolute/path/to/image1.jpg
/absolute/path/to/image2.jpg
```
3. Download the [maskrcnn_resnet50_fpn](https://pytorch.org/vision/0.12/models.html#id70) model from Pytorch's repository of pretrained models. This model requires the torchvision library. To download this model, run the following commands from a Python shell:
```
import torch
from torchvision.models.detection import maskrcnn_resnet50_fpn
model = maskrcnn_resnet50_fpn(pretrained=True)
torch.save(model.state_dict(), 'maskrcnn_resnet50_fpn.pth') # You can replace maskrcnn_resnet50_fpn.pth with your preferred file name for your model state dictionary.
```
4. Note the path to the `OUTPUT` file. This file is used by the pipeline to write the predictions.

### Running `pytorch_image_segmentation.py`

To run the image segmentation pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.pytorch_image_segmentation \
  --input IMAGE_FILE_NAMES \
  --images_dir IMAGES_DIR \
  --output OUTPUT \
  --model_state_dict_path MODEL_STATE_DICT
```
`images_dir` is only needed if your `IMAGE_FILE_NAMES.txt` file contains relative paths (they will be relative from `IMAGES_DIR`).

For example, if you've followed the naming conventions recommended above:
```sh
python -m apache_beam.examples.inference.pytorch_image_segmentation \
  --input IMAGE_FILE_NAMES.txt \
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
## Per Key Image segmentation

[`pytorch_model_per_key_image_segmentation.py`](./pytorch_model_per_key_image_segmentation.py) contains an implementation for a RunInference pipeline that performs image segementation using multiple different trained models based on the `maskrcnn_resnet50_fpn` architecture.

The pipeline reads images, performs basic preprocessing, passes the images to the PyTorch implementation of RunInference, and then writes predictions to a text file.

### Dataset and model for image segmentation

To use this transform, you need a dataset and model for image segmentation. If you've already done the previous example (Image segmentation with pytorch_image_segmentation.py you can reuse the results from some of those setup steps).

1. Create a directory named `IMAGES_DIR`. Create or download images and put them in this directory. The directory is not required if image names in the input file `IMAGE_FILE_NAMES.txt` you create in step 2 have absolute paths.
A popular dataset is from [Coco](https://cocodataset.org/#home). Follow their instructions to download the images.
2. Create a file named `IMAGE_FILE_NAMES.txt` that contains the absolute paths of each of the images in `IMAGES_DIR` that you want to use to run image segmentation. The path to the file can be different types of URIs such as your local file system, an AWS S3 bucket, or a GCP Cloud Storage bucket. For example:
```
/absolute/path/to/image1.jpg
/absolute/path/to/image2.jpg
```
3. Download the [maskrcnn_resnet50_fpn](https://pytorch.org/vision/0.12/models.html#id70) and [maskrcnn_resnet50_fpn_v2](https://pytorch.org/vision/main/models/generated/torchvision.models.detection.maskrcnn_resnet50_fpn_v2.html) models from Pytorch's repository of pretrained models. These models require the torchvision library. To download this model, run the following commands from a Python shell:
```
import torch
from torchvision.models.detection import maskrcnn_resnet50_fpn
from torchvision.models.detection import maskrcnn_resnet50_fpn_v2
model = maskrcnn_resnet50_fpn(pretrained=True)
torch.save(model.state_dict(), 'maskrcnn_resnet50_fpn.pth') # You can replace maskrcnn_resnet50_fpn.pth with your preferred file name for your model state dictionary.
model = maskrcnn_resnet50_fpn_v2(pretrained=True)
torch.save(model.state_dict(), 'maskrcnn_resnet50_fpn_v2.pth') # You can replace maskrcnn_resnet50_fpn_v2.pth with your preferred file name for your model state dictionary.
```
4. Note a path to an `OUTPUT` file that can be used by the pipeline to write the predictions.

### Running `pytorch_model_per_key_image_segmentation.py`

To run the image segmentation pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.pytorch_model_per_key_image_segmentation \
  --input IMAGE_FILE_NAMES \
  --images_dir IMAGES_DIR \
  --output OUTPUT \
  --model_state_dict_paths MODEL_STATE_DICT1,MODEL_STATE_DICT2
```
`images_dir` is only needed if your `IMAGE_FILE_NAMES.txt` file contains relative paths (they will be relative from `IMAGES_DIR`).

For example, if you've followed the naming conventions recommended above:
```sh
python -m apache_beam.examples.inference.pytorch_model_per_key_image_segmentation \
  --input IMAGE_FILE_NAMES.txt \
  --output predictions.csv \
  --model_state_dict_path 'maskrcnn_resnet50_fpn.pth,maskrcnn_resnet50_fpn_v2.pth'
```
This writes the output to the `predictions.csv` with contents like:
```
/Users/dannymccormick/Downloads/images/datasets_coco_raw-data_val2017_000000000139.jpg --- v1 predictions: ['chair', 'tv','potted plant'] --- v2 predictions: ['motorcycle', 'frisbee', 'couch']
...
```
Each image has 2 pieces of associated data - `v1 predictions` and `v2 predictions` corresponding to the version of the model that was used for segmentation.

---
## Object Detection

[`tensorrt_object_detection.py`](./tensorrt_object_detection.py) contains an implementation for a RunInference pipeline that performs object detection using [Tensorflow Object Detection's](https://github.com/tensorflow/models/blob/master/research/object_detection/g3doc/tf2_detection_zoo.md) SSD MobileNet v2 320x320 architecture.

The pipeline reads the images, performs basic preprocessing, passes them to the TensorRT implementation of RunInference, and then writes the predictions to a text file.

### Dataset and model for image classification

You will need to create or download images, and place them into your `IMAGES_DIR` directory. Popular dataset for such task is [COCO dataset](https://cocodataset.org/#home). COCO validation dataset can be obtained [here](http://images.cocodataset.org/zips/val2017.zip).
- **Required**: A path to a file called `IMAGE_FILE_NAMES` that contains the absolute paths of each of the images in `IMAGES_DIR` on which you want to run image segmentation. Paths can be different types of URIs such as your local file system, a AWS S3 bucket or GCP Cloud Storage bucket. For example:
```
/absolute/path/to/000000000139.jpg
/absolute/path/to/000000289594.jpg
```
- **Required**: A path to a file called `TRT_ENGINE` that contains the pre-built TensorRT engine from SSD MobileNet v2 320x320 model. You will need to [follow instructions](https://github.com/NVIDIA/TensorRT/tree/main/samples/python/tensorflow_object_detection_api) on how to download and convert this SSD model into TensorRT engine. At [Create ONNX Graph](https://github.com/NVIDIA/TensorRT/tree/main/samples/python/tensorflow_object_detection_api#create-onnx-graph) step, keep batch size at 1. As soon as you are done with [Build TensorRT Engine](https://github.com/NVIDIA/TensorRT/tree/main/samples/python/tensorflow_object_detection_api#build-tensorrt-engine) step. You can use resulted engine as `TRT_ENGINE` input. In addition, make sure that environment you use for TensorRT engine creation is the same environment you use to run TensorRT inference. It is related not only to TensorRT version, but also to a specific GPU used. Read more about it [here](https://docs.nvidia.com/deeplearning/tensorrt/developer-guide/index.html#compatibility-serialized-engines).

- **Required**: A path to a file called `OUTPUT`, to which the pipeline will write the predictions.
- **Optional**: `IMAGES_DIR`, which is the path to the directory where images are stored. Not required if image names in the input file `IMAGE_FILE_NAMES` have absolute paths.

### Running `tensorrt_object_detection.py`

To run the image classification pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.tensorrt_object_detection \
  --input IMAGE_FILE_NAMES \
  --images_dir IMAGES_DIR \
  --output OUTPUT \
  --engine_path TRT_ENGINE
```
For example:
```sh
python -m apache_beam.examples.inference.tensorrt_object_detection \
  --input image_file_names.txt \
  --output predictions.csv \
  --engine_path ssd_mobilenet_v2_320x320_coco17_tpu-8.trt
```
This writes the output to the `predictions.csv` with contents like:
```
/absolute/path/to/000000000139.jpg;[{'ymin': '217.31875205039978' 'xmin': '295.93122482299805' 'ymax': '315.90323209762573' 'xmax': '357.8959655761719' 'score': '0.72342616' 'class': 'chair'}  {'ymin': '166.81788557767868'.....

/absolute/path/to/000000289594.jpg;[{'ymin': '227.25109100341797' 'xmin': '331.7402381300926'  'ymax': '476.88533782958984' 'xmax': '402.2928895354271' 'score': '0.77217317' 'class': 'person'} {'ymin': '231.8712615966797' 'xmin': '292.8590789437294'.....
...
```
Each line has data separated by a semicolon ";". The first item is the file name. The second item is a list of dictionaries, where each dictionary corresponds with a single detection. A detection contains: box coordinates (ymin, xmin, ymax, xmax); score; and class.

---
## Language modeling

[`pytorch_language_modeling.py`](./pytorch_language_modeling.py) contains an implementation for a RunInference pipeline that performs masked language modeling (that is, decoding a masked token in a sentence) using the `BertForMaskedLM` architecture from Hugging Face.

The pipeline reads sentences, performs basic preprocessing to convert the last word into a `[MASK]` token, passes the masked sentence to the PyTorch implementation of RunInference, and then writes the predictions to a text file.

### Dataset and model for language modeling

To use this transform, you need a dataset and model for language modeling.

1. Download the [BertForMaskedLM](https://huggingface.co/docs/transformers/model_doc/bert#transformers.BertForMaskedLM) model from Hugging Face's repository of pretrained models. You must already have `transformers` installed, then from a Python shell run:
```
import torch
from transformers import BertForMaskedLM
model = BertForMaskedLM.from_pretrained('bert-base-uncased', return_dict=True)
torch.save(model.state_dict(), 'BertForMaskedLM.pth') # You can replace BertForMaskedLM.pth with your preferred file name for your model state dictionary.
```
2. (Optional) Create a file named `SENTENCES.txt` that contains sentences to feed into the model. The content of the file should be similar to the following example:
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
The `input` argument is optional. If none is provided, it will run the pipeline with some
example sentences.

For example, if you've followed the naming conventions recommended above:
```sh
python -m apache_beam.examples.inference.pytorch_language_modeling \
  --input SENTENCES.txt \
  --output predictions.csv \
  --model_state_dict_path BertForMaskedLM.pth
```
Or, using the default example sentences:
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
The first item is the input sentence. The model masks the last word and tries to predict it;
the second item is the word that the model predicts for the mask.

---
## MNIST digit classification
[`sklearn_mnist_classification.py`](./sklearn_mnist_classification.py) contains an implementation for a RunInference pipeline that performs image classification on handwritten digits from the [MNIST](https://en.wikipedia.org/wiki/MNIST_database) database.

The pipeline reads rows of pixels corresponding to a digit, performs basic preprocessing, passes the pixels to the Scikit-learn implementation of RunInference, and then writes the predictions to a text file.

### Dataset and model for MNIST digit classification

To use this transform, you need a dataset and model for MNIST digit classification.

1. Create a file named `INPUT.csv` that contains labels and pixels to feed into the model. Each row should have comma-separated elements. The first element is the label. All other elements are pixel values. The csv should not have column headers. The content of the file should be similar to the following example:
```
1,0,0,0...
0,0,0,0...
1,0,0,0...
4,0,0,0...
...
```
2. Create a file named `MODEL_PATH` that contains the pickled file of a scikit-learn model trained on MNIST data. Please refer to this scikit-learn [model persistence documentation](https://scikit-learn.org/stable/model_persistence.html) on how to serialize models.
3. Update sklearn_examples_requirements.txt to match the version of sklearn used to train the model. Sklearn doesn't guarantee model compatability between versions.


### Running `sklearn_mnist_classification.py`

To run the MNIST classification pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.sklearn_mnist_classification.py \
  --input INPUT \
  --output OUTPUT \
  --model_path MODEL_PATH
```
For example:
```sh
python -m apache_beam.examples.inference.sklearn_mnist_classification.py \
  --input INPUT.csv \
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

### Running `sklearn_japanese_housing_regression.py`

#### Getting the data:
Data for this example can be found at:
https://www.kaggle.com/datasets/nishiodens/japan-real-estate-transaction-prices

#### Models:
Prebuilt sklearn pipelines are hosted at:
https://storage.cloud.google.com/apache-beam-ml/models/japanese_housing/

Note: This example uses more than one model. Since not all features in an example are populated, a different model will be chosen based on available data.

For example, an example without distance to the nearest station will use a model that doesn't rely on that data.

#### Running the Pipeline
To run locally, use the following command:
```sh
python -m apache_beam.examples.inference.sklearn_japanese_housing_regression.py \
  --input_file INPUT \
  --output OUTPUT \
  --model_path MODEL_PATH
```
For example:
```sh
python -m apache_beam.examples.inference.sklearn_japanese_housing_regression.py \
  --input_file housing_examples.csv \
  --output predictions.txt \
  --model_path https://storage.cloud.google.com/apache-beam-ml/models/japanese_housing/
```

This writes the output to the `predictions.txt` with contents like:
```
True Price 40000000.0, Predicted Price 34645912.039208
True Price 34000000.0, Predicted Price 28648634.135857
True Price 31000000.0, Predicted Price 25654277.256461
...
```

---
## Sentiment classification using ONNX version of RoBERTa
[`onnx_sentiment_classification.py`](./onnx_sentiment_classification.py) contains an implementation for a RunInference pipeline that performs sentiment classification on movie reviews.

The pipeline reads rows of txt files corresponding to movie reviews, performs basic preprocessing, passes the pixels to the ONNX version of RoBERTa via RunInference, and then writes the predictions (0 for negative, 1 for positive) to a text file.

### Dataset and model for sentiment classification
We assume you already have a trained model in onnx format. In our example, we use RoBERTa from https://github.com/SeldonIO/seldon-models/blob/master/pytorch/moviesentiment_roberta/pytorch-roberta-onnx.ipynb.

For input data, you can generate your own movie reviews (separated by line breaks) or use IMDB reviews online (https://ai.stanford.edu/~amaas/data/sentiment/).

The output will be a text file, with a binary label (0 for negative, 1 for positive) appended to the review, separated by a semicolon.

### Running the pipeline
To run locally, you can use the following command:
```sh
python -m apache_beam.examples.inference.onnx_sentiment_classification.py \
  --input_file [input file path] \
  --output [output file path] \
  --model_uri [path to onnx model]
```

This writes the output to the output file path with contents like:
```
A comedy-drama of nearly epic proportions rooted in a sincere performance by the title character undergoing midlife crisis .;1
```

---
## MNIST digit classification with Tensorflow
[`tensorflow_mnist_classification.py`](./tensorflow_mnist_classification.py) contains an implementation for a RunInference pipeline that performs image classification on handwritten digits from the [MNIST](https://en.wikipedia.org/wiki/MNIST_database) database.

The pipeline reads rows of pixels corresponding to a digit, performs basic preprocessing(converts the input shape to 28x28), passes the pixels to the trained Tensorflow model with RunInference, and then writes the predictions to a text file.

### Dataset and model for MNIST digit classification

To use this transform, you need a dataset and model for MNIST digit classification.

1. Create a file named [`INPUT.csv`](gs://apache-beam-ml/testing/inputs/it_mnist_data.csv) that contains labels and pixels to feed into the model. Each row should have comma-separated elements. The first element is the label. All other elements are pixel values. The csv should not have column headers. The content of the file should be similar to the following example:
```
1,0,0,0...
0,0,0,0...
1,0,0,0...
4,0,0,0...
...
```
2. Save the trained tensorflow model to a directory `MODEL_DIR` .


### Running `tensorflow_mnist_classification.py`

To run the MNIST classification pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.tensorflow_mnist_classification.py \
  --input INPUT \
  --output OUTPUT \
  --model_path MODEL_DIR
```
For example:
```sh
python -m apache_beam.examples.inference.tensorflow_mnist_classification.py \
  --input INPUT.csv \
  --output predictions.txt \
  --model_path MODEL_DIR
```

This writes the output to the `predictions.txt` with contents like:
```
1,1
4,4
0,0
7,7
3,3
5,5
...
```
Each line has data separated by a comma ",". The first item is the actual label of the digit. The second item is the predicted label of the digit.

---
## Image segmentation with Tensorflow and TensorflowHub

[`tensorflow_imagenet_segmentation.py`](./tensorflow_imagenet_segmentation.py) contains an implementation for a RunInference pipeline that performs image segementation using the [`mobilenet_v2`]("https://tfhub.dev/google/tf2-preview/mobilenet_v2/classification/4") architecture from the tensorflow hub.

The pipeline reads images, performs basic preprocessing, passes the images to the Tensorflow implementation of RunInference, and then writes predictions to a text file.

### Dataset and model for image segmentation

To use this transform, you need a dataset and model for image segmentation.

1. Create a directory named `IMAGE_DIR`. Create or download images and put them in this directory. We
will use the [example image]("https://storage.googleapis.com/download.tensorflow.org/example_images/") on tensorflow.
2. Create a file named `IMAGE_FILE_NAMES.txt` that names of each of the images in `IMAGE_DIR` that you want to use to run image segmentation. For example:
```
grace_hopper.jpg
```
3. A tensorflow `MODEL_PATH`, we will use the [mobilenet]("https://tfhub.dev/google/tf2-preview/mobilenet_v2/classification/4") model.
4. Note the path to the `OUTPUT` file. This file is used by the pipeline to write the predictions.
5. Install TensorflowHub: `pip install tensorflow_hub`

### Running `tensorflow_imagenet_segmentation.py`

To run the image segmentation pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.tensorflow_imagenet_segmentation \
  --input IMAGE_FILE_NAMES \
  --image_dir IMAGES_DIR \
  --output OUTPUT \
  --model_path MODEL_PATH
```

For example, if you've followed the naming conventions recommended above:
```sh
python -m apache_beam.examples.inference.tensorflow_imagenet_segmentation \
  --input IMAGE_FILE_NAMES.txt \
  --image_dir "https://storage.googleapis.com/download.tensorflow.org/example_images/" \
  --output predictions.txt \
  --model_path "https://tfhub.dev/google/tf2-preview/mobilenet_v2/classification/4"
```
This writes the output to the `predictions.txt` with contents like:
```
background
...
```
Each line has a list of predicted label.

---
## MNIST digit classification with Tensorflow using Saved Model Weights
[`tensorflow_mnist_with_weights.py`](./tensorflow_mnist_with_weights.py) contains an implementation for a RunInference pipeline that performs image classification on handwritten digits from the [MNIST](https://en.wikipedia.org/wiki/MNIST_database) database.

The pipeline reads rows of pixels corresponding to a digit, performs basic preprocessing(converts the input shape to 28x28), passes the pixels to the trained Tensorflow model with RunInference, and then writes the predictions to a text file.

The model is loaded from the saved model weights. This can be done by passing a function which creates the model and setting the model type as
`ModelType.SAVED_WEIGHTS` to the `TFModelHandler`. The path to saved weights saved using `model.save_weights(path)` should be passed to the `model_path` argument.

### Dataset and model for MNIST digit classification

To use this transform, you need a dataset and model for MNIST digit classification.

1. Create a file named [`INPUT.csv`](gs://apache-beam-ml/testing/inputs/it_mnist_data.csv) that contains labels and pixels to feed into the model. Each row should have comma-separated elements. The first element is the label. All other elements are pixel values. The csv should not have column headers. The content of the file should be similar to the following example:
```
1,0,0,0...
0,0,0,0...
1,0,0,0...
4,0,0,0...
...
```
2. Save the weights of trained tensorflow model to a directory `SAVED_WEIGHTS_DIR` .


### Running `tensorflow_mnist_with_weights.py`

To run the MNIST classification pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.tensorflow_mnist_with_weights.py \
  --input INPUT \
  --output OUTPUT \
  --model_path SAVED_WEIGHTS_DIR
```
For example:
```sh
python -m apache_beam.examples.inference.tensorflow_mnist_with_weights.py \
  --input INPUT.csv \
  --output predictions.txt \
  --model_path SAVED_WEIGHTS_DIR
```

This writes the output to the `predictions.txt` with contents like:
```
1,1
4,4
0,0
7,7
3,3
5,5
...
```
Each line has data separated by a comma ",". The first item is the actual label of the digit. The second item is the predicted label of the digit.
## Iris Classification

[`xgboost_iris_classification.py`](./xgboost_iris_classification.py) contains an implementation for a RunInference pipeline that performs classification on tabular data from the [Iris Dataset](https://scikit-learn.org/stable/auto_examples/datasets/plot_iris_dataset.html).

The pipeline reads rows that contain the features of a given iris. The features are Sepal Length, Sepal Width, Petal Length and Petal Width. The pipeline passes those features to the XGBoost implementation of RunInference which writes the iris type predictions to a text file.

### Dataset and model for iris classification

To use this transform, you need to have sklearn installed. The dataset is loaded from using sklearn. The `_train_model` function can be used to train a simple classifier. The function outputs it's configuration in a file that can be loaded by the `XGBoostModelHandler`.

### Training a simple classifier

The following function allows you to train a simple classifier using the sklearn Iris dataset. The trained model will be saved in the location passed as a parameter and can then later be loaded in a pipeline using the `XGBoostModelHandler`.
```
import xgboost

from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split


def _train_model(model_state_output_path: str = '/tmp/model.json', seed=999):
  """Function to train an XGBoost Classifier using the sklearn Iris dataset"""
  dataset = load_iris()
  x_train, _, y_train, _ = train_test_split(
      dataset['data'], dataset['target'], test_size=.2, random_state=seed)
  booster = xgboost.XGBClassifier(
      n_estimators=2, max_depth=2, learning_rate=1, objective='binary:logistic')
  booster.fit(x_train, y_train)
  booster.save_model(model_state_output_path)
  return booster
```

#### Running the Pipeline
To run locally, use the following command:

```
python -m apache_beam.examples.inference.xgboost_iris_classification.py \
  --input_type INPUT_TYPE \
  --output OUTPUT_FILE \
  -- model_state MODEL_STATE_JSON \
  [--no_split|--split]
```

For example:

```
python -m apache_beam.examples.inference.xgboost_iris_classification.py \
  --input_type numpy \
  --output predictions.txt \
  --model_state model_state.json \
  --split
```

This writes the output to the `predictions.txt`. Each line contains the batch number and a list with all outputted class labels. There are 3 possible values for class labels: `0`, `1`, and `2`. When each batch contains a single elements the output look like this:
```
0,[1]
1,[2]
2,[1]
3,[0]
...
```

When all elements are in a single batch the output looks like this:
```
0,[1 1 1 0 0 0 0 1 2 0 0 2 0 2 1 2 2 2 2 0 0 0 0 2 2 0 2 2 2 1]

```

## Milk Quality Prediction Windowing Example

`milk_quality_prediction_windowing.py` contains an implementation of a windowing pipeline making use of the RunInference transform. An XGBoost classification the quality of milk based on measurements of pH, temperature, taste, odor, fat, turbidity and color. The model labels a measurement as `bad`, `medium` or `good`. The model is trained on the [Kaggle Milk Quality Prediction dataset](https://www.kaggle.com/datasets/cpluzshrijayan/milkquality).

#### Loading and preprocessing the dataset

The `preprocess_data` function loads the Kaggle dataset from a csv file and splits it into a training and accompanying label set as well as a test set. In typical machine learning setting we would use the training set and the labels to train the model and the test set is used to calculate various metrics such as recall and precision.   We will use the test set data in a test streaming pipeline to showcase the windowing capabilities.

#### Training an XGBoost classifier

The `train_model` function allows you to train a simple XGBoost classifier using the Kaggle Milk Quality Prediction dataset. The trained model will be saved in JSON format at the location passed as a parameter and can then later be used for inference using by loading it via the XGBoostModelhandler.

#### Running the pipeline

```
python -m apache_beam.examples.inference.milk_quality_prediction_windowing.py \
    --dataset \
    <DATASET> \
    --pipeline_input_data \
    <INPUT_DATA> \
    --training_set \
    <TRAINING_SET> \
    --labels \
    <LABELS> \
    --model_state \
    <MODEL_STATE>
```

Where `<DATASET>` is the path to a csv file containing the Kaggle Milk Quality prediction dataset, `<INPUT_DATA>` a filepath to save the data that will be used as input for the streaming pipeline (test set), `<TRAINING_SET>` a filepath to store the training set in csv format, `<LABELS>` a filepath to store the csv containing the labels used to train the model and  `<MODEL_STATE>` the path to the JSON file containing the trained model.
`<INPUT_DATA>`, `<TRAINING_SET>`, and `<LABELS>` will all be parsed from `<DATASET>` and saved before pipeline execution.

Using the test set, we simulate a streaming pipeline that a receives a new measurement of the milk quality parameters every minute. A sliding window keeps track of the measurement of the last 30 minutes and new window starts every 5 minutes. The model predicts the quality of each measurement. After 30 minutes the results are aggregated in a tuple containing the number of measurements that were predicted as bad, medium and high quality samples. The output of each window looks as follows:
```
MilkQualityAggregation(bad_quality_measurements=10, medium_quality_measurements=13, high_quality_measurements=6)
MilkQualityAggregation(bad_quality_measurements=9, medium_quality_measurements=11, high_quality_measurements=4)
MilkQualityAggregation(bad_quality_measurements=8, medium_quality_measurements=7, high_quality_measurements=4)
MilkQualityAggregation(bad_quality_measurements=6, medium_quality_measurements=4, high_quality_measurements=4)
MilkQualityAggregation(bad_quality_measurements=3, medium_quality_measurements=3, high_quality_measurements=3)
MilkQualityAggregation(bad_quality_measurements=1, medium_quality_measurements=2, high_quality_measurements=1)
```

---
## Language modeling with Hugging Face Hub

[`huggingface_language_modeling.py`](./huggingface_language_modeling.py) contains an implementation for a RunInference pipeline that performs masked language modeling (that is, decoding a masked token in a sentence) using the `AutoModelForMaskedLM` architecture from Hugging Face.

The pipeline reads sentences, performs basic preprocessing to convert the last word into a `<mask>` token, passes the masked sentence to the Hugging Face implementation of RunInference, and then writes the predictions to a text file.

### Dataset and model for language modeling

To use this transform, you need a dataset and model for language modeling.

1. Choose a checkpoint to load from Hugging Face Hub, eg:[MaskedLanguageModel](https://huggingface.co/stevhliu/my_awesome_eli5_mlm_model).
2. (Optional) Create a file named `SENTENCES.txt` that contains sentences to feed into the model. The content of the file should be similar to the following example:
```
The capital of France is Paris .
He looked up and saw the sun and stars .
...
```

### Running `huggingface_language_modeling.py`

To run the language modeling pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.huggingface_language_modeling \
  --input SENTENCES \
  --output OUTPUT \
  --model_name REPOSITORY_ID
```
The `input` argument is optional. If none is provided, it will run the pipeline with some
example sentences.

For example, if you've followed the naming conventions recommended above:
```sh
python -m apache_beam.examples.inference.huggingface_language_modeling \
  --input SENTENCES.txt \
  --output predictions.csv \
  --model_name "stevhliu/my_awesome_eli5_mlm_model"
```
Or, using the default example sentences:
```sh
python -m apache_beam.examples.inference.huggingface_language_modeling \
  --output predictions.csv \
  --model_name "stevhliu/my_awesome_eli5_mlm_model"
```

This writes the output to the `predictions.csv` with contents like:
```
The capital of France is Paris .;paris
He looked up and saw the sun and stars .;moon
...
```
Each line has data separated by a semicolon ";".
The first item is the input sentence. The model masks the last word and tries to predict it;
the second item is the word that the model predicts for the mask.

---
## Image classifcation with Vertex AI

[`vertex_ai_image_classification.py`](./vertex_ai_image_classification.py) contains an implementation for a RunInference pipeline that performs image classification using a model hosted on Vertex AI (based on https://cloud.google.com/vertex-ai/docs/tutorials/image-recognition-custom).

The pipeline reads image urls, performs basic preprocessing to convert them into a List of floats, passes the masked sentence to the Vertex AI implementation of RunInference, and then writes the predictions to a text file.

### Dataset and model for image classification

To use this transform, you need a dataset and model hosted on Vertex AI for image classification.

1. Train a model by following the tutorial at https://cloud.google.com/vertex-ai/docs/tutorials/image-recognition-custom
2. Create a file named `IMAGE_FILE_NAMES.txt` that contains the absolute paths of each of the images in `IMAGES_DIR` that you want to use to run image classification. The path to the file can be different types of URIs such as your local file system, an AWS S3 bucket, or a GCP Cloud Storage bucket. For example:
```
/absolute/path/to/image1.jpg
/absolute/path/to/image2.jpg
```

### Running `vertex_ai_image_classification.py`

To run the image classification  pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.vertex_ai_image_classification \
  --endpoint_id '<endpoint of trained model>' \
  --endpoint_project '<gcp project>' \
  --endpoint_region '<gcp region>' \
  --input 'path/to/IMAGE_FILE_NAMES.txt' \
  --output 'path/to/output/file.txt'
```

This writes the output to the output file with contents like:
```
path/to/my/image: tulips (90)
path/to/my/image2: dandelions (78)
...
```
Each line represents a prediction of the flower type along with the confidence in that prediction.

---

## Text classifcation with a Vertex AI LLM

[`vertex_ai_llm_text_classification.py`](./vertex_ai_llm_text_classification.py) contains an implementation for a RunInference pipeline that performs image classification using a model hosted on Vertex AI (based on https://cloud.google.com/vertex-ai/docs/tutorials/image-recognition-custom).

The pipeline reads image urls, performs basic preprocessing to convert them into a List of floats, passes the masked sentence to the Vertex AI implementation of RunInference, and then writes the predictions to a text file.

### Dataset and model for image classification

To use this transform, you need a model hosted on Vertex AI for text classification.
You can get this by tuning the text-bison model following the instructions here -
https://cloud.google.com/vertex-ai/docs/generative-ai/models/tune-models#create_a_model_tuning_job

### Running `vertex_ai_llm_text_classification.py`

To run the text classification  pipeline locally, use the following command:
```sh
python -m apache_beam.examples.inference.vertex_ai_llm_text_classification \
  --endpoint_id '<endpoint of trained LLM>' \
  --endpoint_project '<gcp project>' \
  --endpoint_region '<gcp region>'
```

This writes the output to the output file with contents like:
```
('What is 5+2?', PredictionResult(example={'prompt': 'What is 5+2?'}, inference={'content': '7', 'citationMetadata': {'citations': []}, 'safetyAttributes': {'blocked': False, 'scores': [], 'categories': []}}, model_id='6795590989097467904'))
...
```
Each line represents a tuple containing the example, a [PredictionResult](https://beam.apache.org/releases/pydoc/2.40.0/apache_beam.ml.inference.base.html#apache_beam.ml.inference.base.PredictionResult)
object with the response from the model in the inference field, and the endpoint id representing the model id.
---

## Text completion with vLLM

[`vllm_text_completion.py`](./vllm_text_completion.py) contains an implementation for a RunInference pipeline that performs text completion using a local [vLLM](https://docs.vllm.ai/en/latest/) server.

The pipeline reads in a set of text prompts or past messages, uses RunInference to spin up a local inference server and perform inference, and then writes the predictions to a text file.

### Model for text completion

To use this transform, you can use any [LLM supported by vLLM](https://docs.vllm.ai/en/latest/models/supported_models.html).

### Running `vllm_text_completion.py`

To run the text completion pipeline locally using the Facebook opt 125M model, use the following command.
```sh
python -m apache_beam.examples.inference.vllm_text_completion \
  --model "facebook/opt-125m" \
  --output 'path/to/output/file.txt' \
  <... aditional pipeline arguments to configure runner if not running in GPU environment ...>
```

You will either need to run this locally with a GPU accelerator or remotely on a runner that supports acceleration.
For example, you could run this on Dataflow with a GPU with the following command:

```sh
python -m apache_beam.examples.inference.vllm_text_completion \
  --model "facebook/opt-125m" \
  --output 'gs://path/to/output/file.txt' \
  --runner dataflow \
  --project <gcp project> \
  --region us-central1 \
  --temp_location <temp gcs location> \
  --worker_harness_container_image "gcr.io/apache-beam-testing/beam-ml/vllm:latest" \
  --machine_type "n1-standard-4" \
  --dataflow_service_options "worker_accelerator=type:nvidia-tesla-t4;count:1;install-nvidia-driver:5xx" \
  --staging_location <temp gcs location>
```

Make sure to enable the 5xx driver since vLLM only works with 5xx drivers, not 4xx.

This writes the output to the output file location with contents like:

```
'Hello, my name is', PredictionResult(example={'prompt': 'Hello, my name is'}, inference=Completion(id='cmpl-5f5113a317c949309582b1966511ffc4', choices=[CompletionChoice(finish_reason='length', index=0, logprobs=None, text=' Joel, my dad is Anton Harriman and my wife is Lydia. ', stop_reason=None)], created=1714064548, model='facebook/opt-125m', object='text_completion', system_fingerprint=None, usage=CompletionUsage(completion_tokens=16, prompt_tokens=6, total_tokens=22))})
```
Each line represents a tuple containing the example, a [PredictionResult](https://beam.apache.org/releases/pydoc/2.40.0/apache_beam.ml.inference.base.html#apache_beam.ml.inference.base.PredictionResult) object with the response from the model in the inference field.

You can also choose to run with chat examples. Doing this requires 2 steps:

1) Upload a [chat_template](https://docs.vllm.ai/en/latest/serving/openai_compatible_server.html#chat-template) to a filestore which is accessible from your job's environment (e.g. a public Google Cloud Storage bucket). You can copy [this sample template](https://storage.googleapis.com/apache-beam-ml/additional_files/sample_chat_template.jinja) to get started. You can skip this step if using a model other than `facebook/opt-125m` and you know your model provides a chat template.
2) Add the `--chat true` and `--chat_template <gs://path/to/your/file>` parameters:

```sh
python -m apache_beam.examples.inference.vllm_text_completion \
  --model "facebook/opt-125m" \
  --output 'gs://path/to/output/file.txt' \
  --chat true \
  --chat_template gs://path/to/your/file \
  <... aditional pipeline arguments to configure runner if not running in GPU environment ...>
```

This will configure the pipeline to run against a sequence of previous messages instead of a single text completion prompt.
For example, it might run against:

```
[
    OpenAIChatMessage(role='user', content='What is an example of a type of penguin?'),
    OpenAIChatMessage(role='system', content='An emperor penguin is a type of penguin.'),
    OpenAIChatMessage(role='user', content='Tell me about them')
],
```

and produce the following result in your output file location:

```
An emperor penguin is an adorable creature that lives in Antarctica.
```

---