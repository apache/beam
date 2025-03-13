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

import logging
import unittest
from typing import Dict

import numpy
import pandas

import apache_beam as beam
from apache_beam.ml.anomaly.detectors.custom import CustomDetector
from apache_beam.ml.anomaly.detectors.custom import InputConverter
from apache_beam.ml.anomaly.detectors.custom import OutputConverter
from apache_beam.ml.anomaly.detectors.custom import get_input_type
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerNumpy  # pylint: disable=line-too-long
from apache_beam.ml.inference.sklearn_inference import SklearnModelHandlerPandas  # pylint: disable=line-too-long

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  import torch
  from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor  # pylint: disable=line-too-long
  from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor  # pylint: disable=line-too-long
except ImportError:
  torch = None

try:
  import tensorflow as tf
  from apache_beam.ml.inference.tensorflow_inference import TFModelHandlerNumpy
  from apache_beam.ml.inference.tensorflow_inference import TFModelHandlerTensor
except ImportError:
  tf = None

try:
  import onnx
  from apache_beam.ml.inference.onnx_inference import OnnxModelHandlerNumpy
except ImportError:
  onnx = None

try:
  from apache_beam.ml.inference.huggingface_inference import HuggingFaceModelHandlerTensor  # pylint: disable=line-too-long
  from apache_beam.ml.inference.huggingface_inference import HuggingFaceModelHandlerKeyedTensor  # pylint: disable=line-too-long
  from transformers import AutoModel
  hugging_face = True
except ImportError:
  hugging_face = False

try:
  from apache_beam.ml.inference.tensorrt_inference import TensorRTEngineHandlerNumPy  # pylint: disable=line-too-long
  tensorrt = True
except ImportError:
  tensorrt = False

try:
  import xgboost
  from apache_beam.ml.inference.xgboost_inference import XGBoostModelHandlerNumpy
  from apache_beam.ml.inference.xgboost_inference import XGBoostModelHandlerPandas  # pylint: disable=line-too-long
except ImportError:
  xgboost = None

FAKE_URI = "fake-model-uri"


class ExampleT:
  pass


class PredictionT:
  pass


class ModelT:
  pass


class InputConverterTest(unittest.TestCase):
  def setUp(self):
    self.input_data = beam.Row(a=1, b=2, c=3)

  def test_convert_to_beam_row(self):
    result = InputConverter.convert_to(self.input_data, beam.Row)
    self.assertTrue(isinstance(result, beam.Row))
    self.assertEqual(self.input_data, result)

  def test_convert_to_numpy(self):
    result = InputConverter.convert_to(self.input_data, numpy.ndarray)
    self.assertTrue(isinstance(result, numpy.ndarray))
    self.assertTrue(numpy.array_equal(result, numpy.array([1, 2, 3])))

  def test_convert_to_pandas(self):
    result = InputConverter.convert_to(self.input_data, pandas.DataFrame)
    self.assertTrue(isinstance(result, pandas.DataFrame))
    self.assertTrue(
        result.equals(pandas.DataFrame({
            "a": [1], "b": [2], "c": [3]
        })))

  def test_convert_to_unknown_type(self):
    with self.assertRaises(ValueError):
      InputConverter.convert_to(self.input_data, list)

  @unittest.skipIf(torch is None, "torch is not installed")
  def test_convert_to_torch(self):
    result = InputConverter.convert_to(self.input_data, torch.Tensor)
    self.assertTrue(isinstance(result, torch.Tensor))
    self.assertTrue(torch.equal(result, torch.Tensor([1, 2, 3])))

  @unittest.skipIf(torch is None, "torch is not installed")
  def test_convert_to_keyed_torch(self):
    result = InputConverter.convert_to(self.input_data, Dict[str, torch.Tensor])
    self.assertTrue(isinstance(result, dict))
    expected = {
        'a': torch.Tensor(1), 'b': torch.Tensor(2), 'c': torch.Tensor(3)
    }
    for key, value in result.items():
      self.assertTrue(torch.equal(value, expected[key]))

  @unittest.skipIf(tf is None, "tensorflow is not installed")
  def test_convert_to_tensorflow(self):
    result = InputConverter.convert_to(self.input_data, tf.Tensor)
    self.assertTrue(isinstance(result, tf.Tensor))
    self.assertTrue(tf.reduce_all(tf.equal(result, tf.constant([1, 2, 3]))))

  @unittest.skipIf(tf is None, "tensorflow is not installed")
  def test_convert_to_keyed_tensorflow(self):
    result = InputConverter.convert_to(self.input_data, Dict[str, tf.Tensor])
    self.assertTrue(isinstance(result, dict))
    expected = {'a': tf.constant(1), 'b': tf.constant(2), 'c': tf.constant(3)}
    for key, value in result.items():
      self.assertTrue(
          tf.reduce_all(tf.equal(value, tf.constant(expected[key]))))


class OutputConverterTest(unittest.TestCase):
  def test_convert_from_int(self):
    result = PredictionResult(example=None, inference=10)
    self.assertAlmostEqual(OutputConverter.convert_from(result), 10.0)

  def test_convert_from_float(self):
    result = PredictionResult(example=None, inference=3.14)
    self.assertAlmostEqual(OutputConverter.convert_from(result), 3.14)

  def test_convert_from_numpy_int32(self):
    result = PredictionResult(example=None, inference=numpy.int32(100))
    self.assertAlmostEqual(OutputConverter.convert_from(result), 100.0)

  def test_convert_from_numpy_int64(self):
    result = PredictionResult(example=None, inference=numpy.int64(5))
    self.assertAlmostEqual(OutputConverter.convert_from(result), 5.0)

  def test_convert_from_numpy_float32(self):
    result = PredictionResult(example=None, inference=numpy.float32(2.71))
    self.assertAlmostEqual(OutputConverter.convert_from(result), 2.71)

  @unittest.skipIf(torch is None, "torch is not installed")
  def test_convert_from_torch_tensor(self):
    result = PredictionResult(example=None, inference=torch.tensor(2.5))
    self.assertAlmostEqual(OutputConverter.convert_from(result), 2.5)

  @unittest.skipIf(tf is None, "tensorflow is not installed")
  def test_convert_from_tensorflow_tensor_scalar(self):
    result = PredictionResult(example=None, inference=tf.constant(7))
    self.assertAlmostEqual(OutputConverter.convert_from(result), 7.0)

  @unittest.skipIf(tf is None, "tensorflow is not installed")
  def test_convert_from_tensorflow_tensor_array(self):
    result = PredictionResult(example=None, inference=tf.constant([4.2]))
    self.assertAlmostEqual(OutputConverter.convert_from(result), 4.2, places=6)

  def test_convert_from_unknown_type(self):
    result = PredictionResult(example=None, inference="hello")
    with self.assertRaises(ValueError):
      OutputConverter.convert_from(result)


class CustomDetectorConvertInputTest(unittest.TestCase):
  def setUp(self):
    self.input_data = beam.Row(a=1, b=2, c=3)

  def test_default_convert_input(self):
    class NumpyInputHandler(ModelHandler[numpy.ndarray, PredictionT, ModelT]):
      pass

    detector = CustomDetector(NumpyInputHandler())
    result = detector.convert_input(self.input_data)
    self.assertTrue(numpy.array_equal(result, numpy.array([1, 2, 3])))

  def test_custom_convert_input(self):
    class MyInputData():
      def __init__(self, x, y, z):
        self._x = x
        self._y = y
        self._z = z

      def __eq__(self, other):
        if isinstance(other, MyInputData):
          return self._x == other._x and self._y == other._y and \
            self._z == other._z
        return False

    def beam_row_to_my_data(row: beam.Row) -> MyInputData:
      return MyInputData(x=row.a, y=row.b, z=row.c)

    class MyDataInputHandler(ModelHandler[MyInputData, PredictionT, ModelT]):
      pass

    # without the right input convert fn, it will raise an exception
    detector = CustomDetector(MyDataInputHandler())
    self.assertRaises(ValueError, detector.convert_input, self.input_data)

    detector = CustomDetector(
        MyDataInputHandler(), input_convert_fn=beam_row_to_my_data)
    result = detector.convert_input(self.input_data)
    self.assertEqual(result, MyInputData(1, 2, 3))


class CustomDetectorConvertOutputTest(unittest.TestCase):
  def test_default_convert_output(self):
    class MyHandler(ModelHandler[ExampleT, PredictionT, ModelT]):
      pass

    detector = CustomDetector(MyHandler())
    result = detector.convert_output(
        PredictionResult(example=None, inference=int(10.0)))
    self.assertAlmostEqual(result, 10.0)

  def test_custom_convert_output(self):
    class MyHandler(ModelHandler[ExampleT, PredictionT, ModelT]):
      pass

    class MyOutputData():
      def __init__(self, x, y):
        self._x = x
        self._y = y

    def prediction_to_float(pred: PredictionResult) -> float:
      return pred.inference._y

    detector = CustomDetector(MyHandler())
    model_output = PredictionResult(
        example=None, inference=MyOutputData(12, 34))
    # without the right output convert fn, it will raise an exception
    self.assertRaises(ValueError, detector.convert_output, model_output)

    detector = CustomDetector(
        MyHandler(), output_convert_fn=prediction_to_float)
    result = detector.convert_output(model_output)
    self.assertAlmostEqual(result, 34)


class GetInputTypeTest(unittest.TestCase):
  def test_get_input_type(self):
    class MyHandler(ModelHandler[ExampleT, PredictionT, ModelT]):
      pass

    self.assertEqual(get_input_type(MyHandler()), ExampleT)

  def test_numpy_ndarray_input_model_handlers(self):
    self.assertEqual(
        get_input_type(SklearnModelHandlerNumpy(model_uri=FAKE_URI)),
        numpy.ndarray)
    if tf:
      self.assertEqual(
          get_input_type(TFModelHandlerNumpy(model_uri=FAKE_URI)),
          numpy.ndarray)

    if onnx:
      self.assertEqual(
          get_input_type(OnnxModelHandlerNumpy(model_uri=FAKE_URI)),
          numpy.ndarray)

    if tensorrt:
      self.assertEqual(
          get_input_type(
              TensorRTEngineHandlerNumPy(min_batch_size=1, max_batch_size=1)),
          numpy.ndarray)

    if xgboost:
      self.assertEqual(
          get_input_type(XGBoostModelHandlerNumpy(xgboost.XGBClassifier, "")),
          list)

  def test_panda_dataframe_input_model_handlers(self):
    self.assertEqual(
        get_input_type(SklearnModelHandlerPandas(model_uri=FAKE_URI)),
        pandas.DataFrame)

    if xgboost:
      self.assertEqual(
          get_input_type(XGBoostModelHandlerPandas(xgboost.XGBClassifier, "")),
          pandas.DataFrame)

  def test_tensorflow_tensor_input_model_handlers(self):
    if tf:
      self.assertEqual(
          get_input_type(TFModelHandlerTensor(model_uri=FAKE_URI)), tf.Tensor)

    if hugging_face:
      # TODO: Currently there is no way to tell whether the input of
      # HuggingFaceModelHandlerTensor is tf.Tensor or torch.Tensor before
      # the inference takes place. Use tensorflow tensor by default.
      self.assertEqual(
          get_input_type(
              HuggingFaceModelHandlerTensor(
                  model_uri=FAKE_URI, model_class=AutoModel)),
          tf.Tensor)
      self.assertEqual(
          get_input_type(
              HuggingFaceModelHandlerKeyedTensor(
                  model_uri=FAKE_URI, model_class=AutoModel, framework='tf')),
          Dict[str, tf.Tensor])

  def test_torch_tensor_input_model_handlers(self):
    if torch:
      self.assertEqual(
          get_input_type(PytorchModelHandlerTensor(model_uri=FAKE_URI)),
          torch.Tensor)
      self.assertEqual(
          get_input_type(PytorchModelHandlerKeyedTensor(model_uri=FAKE_URI)),
          Dict[str, torch.Tensor])

    if hugging_face:
      self.assertEqual(
          get_input_type(
              HuggingFaceModelHandlerKeyedTensor(
                  model_uri=FAKE_URI, model_class=AutoModel, framework='pt')),
          Dict[str, torch.Tensor])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
