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

import doctest
import os
import tempfile
import unittest

from apache_beam.dataframe import doctests

SAMPLE_DOCTEST = '''
>>> df = pd.DataFrame({'Animal': ['Falcon', 'Falcon',
...                               'Parrot', 'Parrot'],
...                    'Max Speed': [380., 370., 24., 26.]})
>>> df
   Animal  Max Speed
0  Falcon      380.0
1  Falcon      370.0
2  Parrot       24.0
3  Parrot       26.0
>>> df.groupby(['Animal']).mean()
        Max Speed
Animal
Falcon      375.0
Parrot       25.0
'''

CHECK_USES_DEFERRED_DATAFRAMES = '''
>>> type(pd).__name__
'FakePandasObject'

>>> type(pd.DataFrame([]))
<class 'apache_beam.dataframe.frames.DeferredDataFrame'>

>>> type(pd.DataFrame.from_dict({'a': [1, 2], 'b': [3, 4]}))
<class 'apache_beam.dataframe.frames.DeferredDataFrame'>

>>> pd.Index(range(10))
RangeIndex(start=0, stop=10, step=1)
'''

WONT_IMPLEMENT_RAISING_TESTS = '''
>>> import apache_beam
>>> raise apache_beam.dataframe.frame_base.WontImplementError('anything')
ignored exception
>>> pd.Series(range(10)).__array__()
ignored result
'''

ERROR_RAISING_NAME_ERROR_TESTS = '''
>>> import apache_beam
>>> raise %s('anything')
ignored exception
>>> raise NameError
ignored exception
>>> undefined_name
ignored exception
>>> 2 + 2
4
>>> raise NameError
failed exception
'''

WONT_IMPLEMENT_RAISING_NAME_ERROR_TESTS = ERROR_RAISING_NAME_ERROR_TESTS % (
    'apache_beam.dataframe.frame_base.WontImplementError', )

NOT_IMPLEMENTED_RAISING_TESTS = '''
>>> import apache_beam
>>> raise NotImplementedError('anything')
ignored exception
'''

NOT_IMPLEMENTED_RAISING_NAME_ERROR_TESTS = ERROR_RAISING_NAME_ERROR_TESTS % (
    'NotImplementedError', )

FAILED_ASSIGNMENT = '''
>>> def foo(): raise NotImplementedError()
>>> res = 'old_value'
>>> res = foo()
>>> print(res)
ignored NameError
'''

RST_IPYTHON = '''
Here is an example
.. ipython::

    2 + 2

some multi-line examples

.. ipython::

    def foo(x):
        return x * x
    foo(4)
    foo(
        4
    )

    In [100]: def foo(x):
       ....:     return x * x * x
       ....:
    foo(5)

history is preserved

    foo(3)
    foo(4)

and finally an example with pandas

.. ipython::

    pd.Series([1, 2, 3]).max()


This one should be skipped:

.. ipython::

   @verbatim
   not run or tested

and someting that'll fail (due to fake vs. real pandas)

.. ipython::

   type(pd)
'''


class DoctestTest(unittest.TestCase):
  def test_good(self):
    result = doctests.teststring(SAMPLE_DOCTEST, report=False)
    self.assertEqual(result.attempted, 3)
    self.assertEqual(result.failed, 0)

  def test_failure(self):
    result = doctests.teststring(
        SAMPLE_DOCTEST.replace('25.0', '25.00001'), report=False)
    self.assertEqual(result.attempted, 3)
    self.assertEqual(result.failed, 1)

  def test_uses_beam_dataframes(self):
    result = doctests.teststring(CHECK_USES_DEFERRED_DATAFRAMES, report=False)
    self.assertNotEqual(result.attempted, 0)
    self.assertEqual(result.failed, 0)

  def test_file(self):
    with tempfile.TemporaryDirectory() as dir:
      filename = os.path.join(dir, 'tests.py')
      with open(filename, 'w') as fout:
        fout.write(SAMPLE_DOCTEST)
      result = doctests.testfile(filename, module_relative=False, report=False)
    self.assertEqual(result.attempted, 3)
    self.assertEqual(result.failed, 0)

  def test_file_uses_beam_dataframes(self):
    with tempfile.TemporaryDirectory() as dir:
      filename = os.path.join(dir, 'tests.py')
      with open(filename, 'w') as fout:
        fout.write(CHECK_USES_DEFERRED_DATAFRAMES)
      result = doctests.testfile(filename, module_relative=False, report=False)
    self.assertNotEqual(result.attempted, 0)
    self.assertEqual(result.failed, 0)

  def test_wont_implement(self):
    result = doctests.teststring(
        WONT_IMPLEMENT_RAISING_TESTS,
        optionflags=doctest.ELLIPSIS,
        wont_implement_ok=True)
    self.assertNotEqual(result.attempted, 0)
    self.assertEqual(result.failed, 0)

    result = doctests.teststring(
        WONT_IMPLEMENT_RAISING_TESTS,
        optionflags=doctest.IGNORE_EXCEPTION_DETAIL,
        wont_implement_ok=True)
    self.assertNotEqual(result.attempted, 0)
    self.assertEqual(result.failed, 0)

  def test_wont_implement_followed_by_name_error(self):
    result = doctests.teststring(
        WONT_IMPLEMENT_RAISING_NAME_ERROR_TESTS,
        optionflags=doctest.ELLIPSIS,
        wont_implement_ok=True)
    self.assertEqual(result.attempted, 6)
    self.assertEqual(result.failed, 1)  # Only the very last one.

  def test_not_implemented(self):
    result = doctests.teststring(
        NOT_IMPLEMENTED_RAISING_TESTS,
        optionflags=doctest.ELLIPSIS,
        not_implemented_ok=True)
    self.assertNotEqual(result.attempted, 0)
    self.assertEqual(result.failed, 0)

    result = doctests.teststring(
        NOT_IMPLEMENTED_RAISING_TESTS,
        optionflags=doctest.IGNORE_EXCEPTION_DETAIL,
        not_implemented_ok=True)
    self.assertNotEqual(result.attempted, 0)
    self.assertEqual(result.failed, 0)

  def test_not_implemented_followed_by_name_error(self):
    result = doctests.teststring(
        NOT_IMPLEMENTED_RAISING_NAME_ERROR_TESTS,
        optionflags=doctest.ELLIPSIS,
        not_implemented_ok=True)
    self.assertEqual(result.attempted, 6)
    self.assertEqual(result.failed, 1)  # Only the very last one.

  def test_failed_assignment(self):
    result = doctests.teststring(
        FAILED_ASSIGNMENT,
        optionflags=doctest.ELLIPSIS,
        not_implemented_ok=True)
    self.assertNotEqual(result.attempted, 0)
    self.assertEqual(result.failed, 0)

  def test_rst_ipython(self):
    try:
      import IPython
    except ImportError:
      raise unittest.SkipTest('IPython not available')
    result = doctests.test_rst_ipython(RST_IPYTHON, 'test_rst_ipython')
    self.assertEqual(result.attempted, 8)
    self.assertEqual(result.failed, 1)  # Only the very last one.


if __name__ == '__main__':
  unittest.main()
