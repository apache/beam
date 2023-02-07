#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pytype: skip-file

# beam-playground:
#   name: RunInferenceMultiModel
#   description: Demonstrates cascade model in Apache Beam using the RunInference API.
#   multifile: false
#   context_line: 74
#   categories:
#     - Machine Learning
#   complexity: MEDIUM
#   tags:
#     - runinference
#     - pytorch

import requests
import os
import os.path
import urllib
import json
import io
import subprocess
import shutil
import sys
import logging
from io import BytesIO
from typing import Iterator
from typing import Iterable
from typing import Tuple
from typing import Optional
from typing import Dict
from typing import List
from typing import Any

import apache_beam as beam
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor
from transformers import CLIPProcessor
from transformers import CLIPTokenizer
from transformers import CLIPModel
from transformers import CLIPConfig
from transformers import CLIPFeatureExtractor
import torch
from torchvision import transforms
from torchvision.transforms.functional import InterpolationMode
import numpy as np
import matplotlib.pyplot as plt
from PIL import Image


dir_blip = os.getcwd() + "/BLIP"
dir_clip = os.getcwd() + '/clip-vit-base-patch32'

print("Installing CLIP dependencies\n")
_ = subprocess.run(
    ['git', 'clone', 'https://huggingface.co/openai/clip-vit-base-patch32'])
clip_feature_extractor_config_path = os.getcwd(
) + '/clip-vit-base-patch32/preprocessor_config.json'
clip_tokenizer_vocab_config_path = os.getcwd() + '/clip-vit-base-patch32/vocab.json'
clip_merges_config_path = os.getcwd() + '/clip-vit-base-patch32/merges.txt'
clip_model_config_path = os.getcwd() + '/clip-vit-base-patch32/config.json'
clip_state_dict_path = os.getcwd() + '/clip-vit-base-patch32/pytorch_model.bin'

print("Installing BLIP dependencies\n")
_ = subprocess.run(['git', 'clone', 'https://github.com/salesforce/BLIP'])
os.chdir(dir_blip)
sys.path.append(dir_blip)
from models.blip import blip_decoder
if not os.path.exists('model*_base_caption.pth'):
  _ = subprocess.run(
      ['gdown', 'https://storage.googleapis.com/sfr-vision-language-research/BLIP/models/model*_base_caption.pth'])
blip_state_dict_path = 'blip_state_dict.pth'
torch.save(torch.load('model*_base_caption.pth')
           ['model'], blip_state_dict_path)

print("Installing I/O helper functions\n")


class ReadImagesFromUrl(beam.DoFn):
  """
  Read an image from a given URL and return a tuple of the images_url
  and image data.
  """

  def process(self, element: str) -> Tuple[str, Image.Image]:
    response = requests.get(element)
    image = Image.open(BytesIO(response.content)).convert('RGB')
    return [(element, image)]


class FormatCaptions(beam.DoFn):
  """
  Print the image name and its most relevant captions after CLIP ranking.
  """

  def __init__(self, number_of_top_captions: int):
    self._number_of_top_captions = number_of_top_captions

  def process(self, element: Tuple[str, List[str]]):
    image_url, caption_list = element
    caption_list = caption_list[:self._number_of_top_captions]
    img_name = os.path.basename(image_url).rsplit('.')[0]
    print(f'Image: {img_name}')
    print(f'\tTop {self._number_of_top_captions} captions ranked by CLIP:')
    for caption_rank, caption_prob_pair in enumerate(caption_list):
      print(
          f'\t\t{caption_rank+1}: {caption_prob_pair[0]}. (Caption probability: {caption_prob_pair[1]:.2f})')
    print('\n')


# Define the preprocessing and postprocessing functions for each of the models.
print("Defining BLIP functions")


class PreprocessBLIPInput(beam.DoFn):

  """
  Process the raw image input to a format suitable for BLIP inference. The processed
  images are duplicated to the number of desired captions per image. 

  Preprocessing transformation taken from: 
  https://github.com/salesforce/BLIP/blob/d10be550b2974e17ea72e74edc7948c9e5eab884/predict.py
  """

  def __init__(self, captions_per_image: int):
    self._captions_per_image = captions_per_image

  def setup(self):

    # Initialize the image transformer.
    self._transform = transforms.Compose([
        transforms.Resize((384, 384), interpolation=InterpolationMode.BICUBIC),
        transforms.ToTensor(),
        transforms.Normalize((0.48145466, 0.4578275, 0.40821073),
                             (0.26862954, 0.26130258, 0.27577711))
    ])

  def process(self, element):
    image_url, image = element
    # The following lines provide a workaround to turn off BatchElements.
    preprocessed_img = self._transform(image).unsqueeze(0)
    preprocessed_img = preprocessed_img.repeat(
        self._captions_per_image, 1, 1, 1)
    # Parse the processed input to a dictionary to a format suitable for RunInference.
    preprocessed_dict = {'inputs': preprocessed_img}

    return [(image_url, preprocessed_dict)]


class PostprocessBLIPOutput(beam.DoFn):
  """
  Process the PredictionResult to get the generated image captions
  """

  def process(self, element: Tuple[str, Iterable[PredictionResult]]):
    image_url, prediction = element

    return [(image_url, prediction.inference)]


print("Defining CLIP functions")


class PreprocessCLIPInput(beam.DoFn):

  """
  Process the image-caption pair to a format suitable for CLIP inference. 

  After grouping the raw images with the generated captions, we need to 
  preprocess them before passing them to the ranking stage (CLIP model).
  """

  def __init__(self,
               feature_extractor_config_path: str,
               tokenizer_vocab_config_path: str,
               merges_file_config_path: str):

    self._feature_extractor_config_path = feature_extractor_config_path
    self._tokenizer_vocab_config_path = tokenizer_vocab_config_path
    self._merges_file_config_path = merges_file_config_path

  def setup(self):

    # Initialize the CLIP feature extractor.
    feature_extractor_config = CLIPConfig.from_pretrained(
        self._feature_extractor_config_path)
    feature_extractor = CLIPFeatureExtractor(feature_extractor_config)

    # Initialize the CLIP tokenizer.
    tokenizer = CLIPTokenizer(self._tokenizer_vocab_config_path,
                              self._merges_file_config_path)

    # Initialize the CLIP processor used to process the image-caption pair.
    self._processor = CLIPProcessor(feature_extractor=feature_extractor,
                                    tokenizer=tokenizer)

  def process(self, element: Tuple[str, Dict[str, List[Any]]]):

    image_url, image_captions_pair = element
    # Unpack the image and captions after grouping them with 'CoGroupByKey()'.
    image = image_captions_pair['image'][0]
    captions = image_captions_pair['captions'][0]
    preprocessed_clip_input = self._processor(images=image,
                                              text=captions,
                                              return_tensors="pt",
                                              padding=True)

    image_url_caption_pair = (image_url, captions)
    return [(image_url_caption_pair, preprocessed_clip_input)]


class RankCLIPOutput(beam.DoFn):
  """
  Process the output of CLIP to get the captions sorted by ranking order.

  The logits are the output of the CLIP model. Here, we apply a softmax activation
  function to the logits to get the probabilistic distribution of the relevance
  of each caption to the target image. After that, we sort the captions in descending
  order with respect to the probabilities as a caption-probability pair. 
  """

  def process(self, element: Tuple[Tuple[str, List[str]], Iterable[PredictionResult]]):
    (image_url, captions), prediction = element
    prediction_results = prediction.inference
    prediction_probs = prediction_results.softmax(
        dim=-1).cpu().detach().numpy()
    ranking = np.argsort(-prediction_probs)
    sorted_caption_prob_pair = [
        (captions[idx], prediction_probs[idx]) for idx in ranking]

    return [(image_url, sorted_caption_prob_pair)]

# Model handlers. Use a KeyedModelHandler for both models to attach a key to the general ModelHandler


class PytorchNoBatchModelHandlerKeyedTensor(PytorchModelHandlerKeyedTensor):
  """Wrapper to PytorchModelHandler to limit batch size to 1.
The caption strings generated from the BLIP tokenizer might have different
lengths. Different length strings don't work with torch.stack() in the current RunInference
implementation, because stack() requires tensors to be the same size.
Restricting max_batch_size to 1 means there is only 1 example per `batch`
in the run_inference() call.
"""
  # The following lines provide a workaround to turn off BatchElements.

  def batch_elements_kwargs(self):
    return {'max_batch_size': 1}


print("Generating captions with BLIP")

MAX_CAPTION_LENGTH = 80
MIN_CAPTION_LENGTH = 10
# Increasing Beam search might improve the quality of the captions,
# but also results in more compute time
NUM_BEAMS = 1


class BLIPWrapper(torch.nn.Module):
  """
   Wrapper around the BLIP model to overwrite the default "forward" method with the "generate" method, because BLIP uses the 
  "generate" method to produce the image captions.
  """

  def __init__(self, base_model: blip_decoder, num_beams: int, max_length: int,
               min_length: int):
    super().__init__()
    self._model = base_model()
    self._num_beams = num_beams
    self._max_length = max_length
    self._min_length = min_length

  def forward(self, inputs: torch.Tensor):
    # Squeeze because RunInference adds an extra dimension, which is empty.
    # The following lines provide a workaround to turn off BatchElements.
    inputs = inputs.squeeze(0)
    captions = self._model.generate(inputs,
                                    sample=True,
                                    num_beams=self._num_beams,
                                    max_length=self._max_length,
                                    min_length=self._min_length)
    return [captions]

  def load_state_dict(self, state_dict: dict):
    self._model.load_state_dict(state_dict)


BLIP_model_handler = PytorchNoBatchModelHandlerKeyedTensor(
    state_dict_path=blip_state_dict_path,
    model_class=BLIPWrapper,
    model_params={'base_model': blip_decoder, 'num_beams': NUM_BEAMS,
                  'max_length': MAX_CAPTION_LENGTH, 'min_length': MIN_CAPTION_LENGTH},
    device='CPU')

BLIP_keyed_model_handler = KeyedModelHandler(BLIP_model_handler)

print("Ranking captions with CLIP")


class CLIPWrapper(CLIPModel):

  def forward(self, **kwargs: Dict[str, torch.Tensor]):
    # Squeeze because RunInference adds an extra dimension, which is empty.
    # The following lines provide a workaround to turn off BatchElements.
    kwargs = {key: tensor.squeeze(0) for key, tensor in kwargs.items()}
    output = super().forward(**kwargs)
    logits = output.logits_per_image
    return logits


CLIP_model_handler = PytorchNoBatchModelHandlerKeyedTensor(
    state_dict_path=clip_state_dict_path,
    model_class=CLIPWrapper,
    model_params={'config': CLIPConfig.from_pretrained(
        clip_model_config_path)},
    device='CPU')

CLIP_keyed_model_handler = KeyedModelHandler(CLIP_model_handler)


def main():
  print("Running pipeline\n")
  images_url = ['https://storage.googleapis.com/apache-beam-samples/image_captioning/Paris-sunset.jpeg',
                'https://storage.googleapis.com/apache-beam-samples/image_captioning/Wedges.jpeg',
                'https://storage.googleapis.com/apache-beam-samples/image_captioning/Hamsters.jpeg']

  print('Input images:\n'
        'https://storage.googleapis.com/apache-beam-samples/image_captioning/Paris-sunset.jpeg\n'
        'https://storage.googleapis.com/apache-beam-samples/image_captioning/Wedges.jpeg\n'
        'https://storage.googleapis.com/apache-beam-samples/image_captioning/Hamsters.jpeg\n')
  # Number of captions generated per image.
  NUM_CAPTIONS_PER_IMAGE = 10

  # Number of top captions to display.
  NUM_TOP_CAPTIONS_TO_DISPLAY = 3
  with beam.Pipeline() as pipeline:

    read_images = (
        pipeline
        | "ReadUrl" >> beam.Create(images_url)
        | "ReadImages" >> beam.ParDo(ReadImagesFromUrl()))

    blip_caption_generation = (
        read_images
        | "PreprocessBlipInput" >> beam.ParDo(PreprocessBLIPInput(NUM_CAPTIONS_PER_IMAGE))
        | "GenerateCaptions" >> RunInference(BLIP_keyed_model_handler)
        | "PostprocessCaptions" >> beam.ParDo(PostprocessBLIPOutput()))

    clip_captions_ranking = (
        ({'image': read_images, 'captions': blip_caption_generation})
        | "CreateImageCaptionPair" >> beam.CoGroupByKey()
        | "PreprocessClipInput" >> beam.ParDo(
            PreprocessCLIPInput(
                clip_feature_extractor_config_path,
                clip_tokenizer_vocab_config_path,
                clip_merges_config_path))
        | "GetRankingLogits" >> RunInference(CLIP_keyed_model_handler)
        | "RankClipOutput" >> beam.ParDo(RankCLIPOutput()))

    clip_captions_ranking | "FormatCaptions" >> beam.ParDo(
        FormatCaptions(NUM_TOP_CAPTIONS_TO_DISPLAY))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  try:
    main()
  finally:
    if os.path.exists(dir_clip):
      pass
      shutil.rmtree(dir_clip)
    if os.path.exists(dir_blip):
      pass
      shutil.rmtree(dir_blip)
