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

import types
import unittest

import pytest

from apache_beam.ml.rag.types import EmbeddableItem

pytest.importorskip("vertexai", reason="Vertex AI dependencies not available")

# pylint: disable=wrong-import-position
from vertexai.vision_models import Image
from vertexai.vision_models import Video
from vertexai.vision_models import VideoSegmentConfig

from apache_beam.ml.transforms.embeddings import vertex_ai


class VertexAIMultiModalOutputFnTest(unittest.TestCase):
  def test_output_fn_does_not_modify_input(self):
    image = vertex_ai.VertexImage(image_content=Image(image_bytes=b'img'))
    video = vertex_ai.VertexVideo(
        video_content=Video(video_bytes=b'vid'), config=VideoSegmentConfig())
    text = EmbeddableItem.from_text('hello')
    item = {'image': image, 'video': video, 'text': text, 'id': 1}
    video_embeddings = ['video-embedding']
    response = types.SimpleNamespace(
        image_embedding=[0.1, 0.2],
        video_embeddings=video_embeddings,
        text_embedding=[0.3, 0.4])

    output = vertex_ai._multimodal_dict_output_fn(
        'image', 'video', 'text', [item], [response])

    self.assertEqual(len(output), 1)
    self.assertEqual(output[0]['image'].embedding, [0.1, 0.2])
    self.assertEqual(output[0]['video'].embeddings, video_embeddings)
    self.assertEqual(output[0]['text'].embedding.dense_embedding, [0.3, 0.4])
    self.assertEqual(output[0]['id'], 1)
    # The input element and its nested objects are unchanged.
    self.assertIs(item['image'], image)
    self.assertIs(item['video'], video)
    self.assertIs(item['text'], text)
    self.assertIsNone(image.embedding)
    self.assertIsNone(video.embeddings)
    self.assertIsNone(text.embedding)


if __name__ == '__main__':
  unittest.main()
