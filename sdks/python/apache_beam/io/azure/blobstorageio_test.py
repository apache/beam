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

"""Tests for Azure Blob Storage client.
"""
# pytype: skip-file

from __future__ import absolute_import

import logging
import os
import time
import unittest

from nose.plugins.attrib import attr

# Protect against environments where azure library is not available.
try:
  from apache_beam.io.azure import blobstorageio
except ImportError:
  blobstorageio = None  # type: ignore[assignment]
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(blobstorageio is None, 'Azure dependencies are not installed')
class TestAZFSPathParser(unittest.TestCase):

  BAD_AZFS_PATHS = [
      'azfs://'
      'azfs://storage-account/'
      'azfs://storage-account/**'
      'azfs://storage-account/**/*'
      'azfs://container'
      'azfs:///name'
      'azfs:///'
      'azfs:/blah/container/name'
      'azfs://ab/container/name'
      'azfs://accountwithmorethan24chars/container/name'
      'azfs://***/container/name'
      'azfs://storageaccount/my--container/name'
      'azfs://storageaccount/CONTAINER/name'
      'azfs://storageaccount/ct/name'
  ]

  def test_azfs_path(self):
    self.assertEqual(
        blobstorageio.parse_azfs_path(
            'azfs://storageaccount/container/name', get_account=True),
        ('storageaccount', 'container', 'name'))
    self.assertEqual(
        blobstorageio.parse_azfs_path(
            'azfs://storageaccount/container/name/sub', get_account=True),
        ('storageaccount', 'container', 'name/sub'))

  def test_bad_azfs_path(self):
    for path in self.BAD_AZFS_PATHS:
      self.assertRaises(ValueError, blobstorageio.parse_azfs_path, path)
    self.assertRaises(
        ValueError,
        blobstorageio.parse_azfs_path,
        'azfs://storageaccount/container/')

  def test_azfs_path_blob_optional(self):
    self.assertEqual(
        blobstorageio.parse_azfs_path(
            'azfs://storageaccount/container/name',
            blob_optional=True,
            get_account=True), ('storageaccount', 'container', 'name'))
    self.assertEqual(
        blobstorageio.parse_azfs_path(
            'azfs://storageaccount/container/',
            blob_optional=True,
            get_account=True), ('storageaccount', 'container', ''))

  def test_bad_azfs_path_blob_optional(self):
    for path in self.BAD_AZFS_PATHS:
      self.assertRaises(ValueError, blobstorageio.parse_azfs_path, path, True)


class TestBlobStorageIO(unittest.TestCase):
  def _insert_random_file(self, path, size):
    contents = os.urandom(size)

    # Insert file.
    f = self.azfs.open(path, 'w')
    f.write(contents)
    f.close()

    return contents

  def setUp(self):
    connect_str = (
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFs" +
        "uFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;")
    self.azfs = blobstorageio.BlobStorageIO(connect_str, True)
    try:
      self.azfs.client.create_container("gsoc")
    except: # pylint: disable=bare-except
      pass
    self.TEST_DATA_PATH = 'azfs://devstoreaccount1/gsoc/'

  @attr('AzuriteIT')
  def test_file_mode(self):
    file_name = self.TEST_DATA_PATH + 'sloth/pictures/sleeping'
    with self.azfs.open(file_name, 'w') as f:
      assert f.mode == 'w'
    with self.azfs.open(file_name, 'r') as f:
      assert f.mode == 'r'

    # Clean up.
    self.azfs.delete(file_name)

  @attr('AzuriteIT')
  def test_full_file_read(self):
    file_name = self.TEST_DATA_PATH + 'test_file_read'
    file_size = 1024

    test_file_contents = self._insert_random_file(file_name, file_size)

    test_file = self.azfs.open(file_name)
    self.assertEqual(test_file.mode, 'r')
    test_file.seek(0, os.SEEK_END)
    self.assertEqual(test_file.tell(), file_size)
    self.assertEqual(test_file.read(), b'')
    test_file.seek(0)
    self.assertEqual(test_file.read(), test_file_contents)

    # Clean up.
    self.azfs.delete(file_name)

  @attr('AzuriteIT')
  def test_file_write(self):
    file_name = self.TEST_DATA_PATH + 'test_file_write'
    # file_size = 5 * 1024 * 1024 + 2000
    file_size = 1024
    contents = os.urandom(file_size)
    f = self.azfs.open(file_name, 'w')
    self.assertEqual(f.mode, 'w')
    f.write(contents[0:1000])
    f.write(contents[1000:1024])
    f.close()
    test_file = self.azfs.open(file_name, 'r')
    test_file_contents = test_file.read()
    self.assertEqual(test_file_contents, contents)

    # Clean up.
    self.azfs.delete(file_name)

  @attr('AzuriteIT')
  def test_copy(self):
    src_file_name = self.TEST_DATA_PATH + 'mysource'
    dest_file_name = self.TEST_DATA_PATH + 'mydest'
    file_size = 1024
    self._insert_random_file(src_file_name, file_size)

    self.assertTrue(src_file_name in self.azfs.list_prefix(self.TEST_DATA_PATH))
    self.assertFalse(
        dest_file_name in self.azfs.list_prefix(self.TEST_DATA_PATH))

    self.azfs.copy(src_file_name, dest_file_name)

    self.assertTrue(src_file_name in self.azfs.list_prefix(self.TEST_DATA_PATH))
    self.assertTrue(
        dest_file_name in self.azfs.list_prefix(self.TEST_DATA_PATH))

    self.azfs.delete(src_file_name)
    self.azfs.delete(dest_file_name)

    # Test copy of non-existent files.
    with self.assertRaises(blobstorageio.BlobStorageError) as err:
      self.azfs.copy(
          self.TEST_DATA_PATH + 'non-existent',
          self.TEST_DATA_PATH + 'non-existent-destination')

    self.assertTrue(
        'The specified blob does not exist.' in err.exception.message)
    self.assertEqual(err.exception.code, 404)

  @attr('AzuriteIT')
  def test_copy_tree(self):
    src_dir_name = self.TEST_DATA_PATH + 'mysource/'
    dest_dir_name = self.TEST_DATA_PATH + 'mydest/'
    file_size = 1024
    paths = ['a', 'b/c', 'b/d']
    for path in paths:
      src_file_name = src_dir_name + path
      dest_file_name = dest_dir_name + path
      self._insert_random_file(src_file_name, file_size)
      self.assertTrue(
          src_file_name in self.azfs.list_prefix(self.TEST_DATA_PATH))
      self.assertFalse(
          dest_file_name in self.azfs.list_prefix(self.TEST_DATA_PATH))

    results = self.azfs.copy_tree(src_dir_name, dest_dir_name)

    for src_file_name, dest_file_name, err in results:
      self.assertTrue(src_dir_name in src_file_name)
      self.assertTrue(dest_dir_name in dest_file_name)
      self.assertIsNone(err)

      self.assertTrue(
          src_file_name in self.azfs.list_prefix(self.TEST_DATA_PATH))
      self.assertTrue(
          dest_file_name in self.azfs.list_prefix(self.TEST_DATA_PATH))

    # Clean up.
    for path in paths:
      src_file_name = src_dir_name + path
      dest_file_name = dest_dir_name + path
      self.azfs.delete(src_file_name)
      self.azfs.delete(dest_file_name)

  @attr('AzuriteIT')
  def test_copy_paths(self):
    from_name_pattern = self.TEST_DATA_PATH + 'copy_me_%d'
    to_name_pattern = self.TEST_DATA_PATH + 'destination_%d'
    file_size = 1024
    num_files = 10

    src_dest_pairs = [(from_name_pattern % i, to_name_pattern % i)
                      for i in range(num_files)]

    # Execute batch copy of nonexistent files.
    result = self.azfs.copy_paths(src_dest_pairs)

    self.assertTrue(result)
    for i, (src, dest, exception) in enumerate(result):
      self.assertEqual(src, from_name_pattern % i)
      self.assertEqual(dest, to_name_pattern % i)
      self.assertTrue(isinstance(exception, blobstorageio.BlobStorageError))
      self.assertEqual(exception.code, 404)
      self.assertFalse(self.azfs.exists(from_name_pattern % i))
      self.assertFalse(self.azfs.exists(to_name_pattern % i))

    # Insert files.
    for i in range(num_files):
      self._insert_random_file(from_name_pattern % i, file_size)

    # Check if files were inserte properly.
    for i in range(num_files):
      self.assertTrue(self.azfs.exists(from_name_pattern % i))

    # Execute batch copy.
    result = self.azfs.copy_paths(src_dest_pairs)

    # Check files copied properly.
    for i in range(num_files):
      self.assertTrue(self.azfs.exists(from_name_pattern % i))
      self.assertTrue(self.azfs.exists(to_name_pattern % i))

    # Check results.
    for i, (src, dest, exception) in enumerate(result):
      self.assertEqual(src_dest_pairs[i], (src, dest))
      self.assertEqual(exception, None)

    # Clean up.
    for src, dest in src_dest_pairs:
      self.azfs.delete(src)
      self.azfs.delete(dest)

  @attr('AzuriteIT')
  def test_rename(self):
    src_file_name = self.TEST_DATA_PATH + 'mysource'
    dest_file_name = self.TEST_DATA_PATH + 'mydest'
    file_size = 1024

    self._insert_random_file(src_file_name, file_size)
    self.assertTrue(src_file_name in self.azfs.list_prefix(self.TEST_DATA_PATH))
    self.assertFalse(
        dest_file_name in self.azfs.list_prefix(self.TEST_DATA_PATH))

    self.azfs.rename(src_file_name, dest_file_name)

    self.assertFalse(
        src_file_name in self.azfs.list_prefix(self.TEST_DATA_PATH))
    self.assertTrue(
        dest_file_name in self.azfs.list_prefix(self.TEST_DATA_PATH))

    # Clean up.
    self.azfs.delete(src_file_name)
    self.azfs.delete(dest_file_name)

  @attr('AzuriteIT')
  def test_rename_files(self):
    from_name_pattern = self.TEST_DATA_PATH + 'to_rename_%d'
    to_name_pattern = self.TEST_DATA_PATH + 'been_renamed_%d'
    file_size = 1024
    num_files = 10

    src_dest_pairs = [(from_name_pattern % i, to_name_pattern % i)
                      for i in range(num_files)]

    result = self.azfs.rename_files(src_dest_pairs)

    self.assertTrue(result)
    for i, (src, dest, exception) in enumerate(result):
      self.assertEqual(src, from_name_pattern % i)
      self.assertEqual(dest, to_name_pattern % i)
      self.assertTrue(isinstance(exception, blobstorageio.BlobStorageError))
      self.assertEqual(exception.code, 404)
      self.assertFalse(self.azfs.exists(from_name_pattern % i))
      self.assertFalse(self.azfs.exists(to_name_pattern % i))

    # Insert files.
    for i in range(num_files):
      self._insert_random_file(from_name_pattern % i, file_size)

    # Check files inserted properly.
    for i in range(num_files):
      self.assertTrue(self.azfs.exists(from_name_pattern % i))
      self.assertFalse(self.azfs.exists(to_name_pattern % i))

    # Execute batch rename.
    self.azfs.rename_files(src_dest_pairs)

    # Check files were renamed properly.
    for i in range(num_files):
      self.assertFalse(self.azfs.exists(from_name_pattern % i))
      self.assertTrue(self.azfs.exists(to_name_pattern % i))

    # Clean up.
    for src, dest in src_dest_pairs:
      self.azfs.delete(src)
      self.azfs.delete(dest)

  @attr('AzuriteIT')
  def test_rename_directory_error(self):
    dir_name = self.TEST_DATA_PATH + 'rename_dir/'
    file_name = dir_name + 'test_rename_file'
    file_size = 1024

    # Insert file.
    self._insert_random_file(file_name, file_size)

    # Check file inserted properly.
    self.assertTrue(self.azfs.exists(file_name))

    # Execute rename operation.
    with self.assertRaises(ValueError):
      self.azfs.rename_files([(file_name, self.TEST_DATA_PATH + 'dir_dest/')])

    # Clean up.
    self.azfs.delete(file_name)

  @attr('AzuriteIT')
  def test_exists(self):
    file_name = self.TEST_DATA_PATH + 'test_exists'
    file_size = 1024

    self.assertFalse(self.azfs.exists(file_name))
    self._insert_random_file(file_name, file_size)
    self.assertTrue(self.azfs.exists(file_name))

    # Clean up.
    self.azfs.delete(file_name)
    self.assertFalse(self.azfs.exists(file_name))

  @attr('AzuriteIT')
  def test_size(self):
    file_name = self.TEST_DATA_PATH + 'test_size'
    file_size = 1024

    self._insert_random_file(file_name, file_size)
    self.assertTrue(self.azfs.exists(file_name))
    self.assertEqual(self.azfs.size(file_name), file_size)
    self.assertNotEqual(self.azfs.size(file_name), 19)

    # Clean up.
    self.azfs.delete(file_name)
    self.assertFalse(self.azfs.exists(file_name))

  @attr('AzuriteIT')
  def test_last_updated(self):
    file_name = self.TEST_DATA_PATH + 'test_last_updated'
    file_size = 1024
    tolerance = 60

    self._insert_random_file(file_name, file_size)
    self.assertTrue(self.azfs.exists(file_name))
    self.assertAlmostEqual(
        self.azfs.last_updated(file_name), time.time(), delta=tolerance)

    # Clean up.
    self.azfs.delete(file_name)

  @attr('AzuriteIT')
  def test_checksum(self):
    file_name = self.TEST_DATA_PATH + 'test_checksum'
    file_size = 1024

    test_file_contents = self._insert_random_file(file_name, file_size)
    original_etag = self.azfs.checksum(file_name)

    f = self.azfs.open(file_name, 'w')
    self.assertEqual(f.mode, 'w')
    f.write(test_file_contents)
    f.close()
    rewritten_etag = self.azfs.checksum(file_name)

    self.assertNotEqual(original_etag, rewritten_etag)

    # Clean up.
    self.azfs.delete(file_name)

  @attr('AzuriteIT')
  def test_delete(self):
    file_name = self.TEST_DATA_PATH + 'test_delete'
    file_size = 1024

    # Test deletion of non-existent file.
    self.azfs.delete(file_name)

    self._insert_random_file(file_name, file_size)
    files = self.azfs.list_prefix(self.TEST_DATA_PATH)
    self.assertTrue(file_name in files)

    self.azfs.delete(file_name)
    self.assertFalse(self.azfs.exists(file_name))

  @attr('AzuriteIT')
  def test_delete_paths(self):
    # Create files.
    prefix = self.TEST_DATA_PATH + 'test_delete_paths/'
    file_names = [prefix + 'x', prefix + 'y/z']
    file_size = 1024
    for file_name in file_names:
      self._insert_random_file(file_name, file_size)

    self.assertTrue(self.azfs.exists(file_names[0]))
    self.assertTrue(self.azfs.exists(file_names[1]))

    # Delete paths.
    paths = [prefix + 'x', prefix + 'y/']
    self.azfs.delete_paths(paths)

    self.assertFalse(self.azfs.exists(file_names[0]))
    self.assertFalse(self.azfs.exists(file_names[1]))

  @attr('AzuriteIT')
  def test_delete_tree(self):
    root_path = self.TEST_DATA_PATH + 'test_delete_tree/'
    leaf_paths = ['x', 'y/a', 'y/b', 'y/b/c']

    # Create path names.
    paths = [root_path + leaf for leaf in leaf_paths]

    file_size = 1024
    # Create file tree.
    for path in paths:
      self._insert_random_file(path, file_size)

    # Files should exist.
    for path in paths:
      self.assertTrue(self.azfs.exists(path))

    # Delete the tree.
    self.azfs.delete_tree(root_path)

    # Files shouldn't exist.
    for path in paths:
      self.assertFalse(self.azfs.exists(path))

  @attr('AzuriteIT')
  def test_delete_files(self):
    file_name_pattern = self.TEST_DATA_PATH + 'test_delete_files/%d'
    file_size = 1024
    num_files = 5

    # Test deletion of non-existent files.
    result = self.azfs.delete_files(
        [file_name_pattern % i for i in range(num_files)])
    self.assertTrue(result)
    for i, (file_name, exception) in enumerate(result):
      self.assertEqual(file_name, file_name_pattern % i)
      self.assertEqual(exception, 404)
      self.assertFalse(self.azfs.exists(file_name_pattern % i))

    # Insert files.
    for i in range(num_files):
      self._insert_random_file(file_name_pattern % i, file_size)

    # Check files inserted properly.
    for i in range(num_files):
      self.assertTrue(self.azfs.exists(file_name_pattern % i))

    # Execute batch delete.
    self.azfs.delete_files([file_name_pattern % i for i in range(num_files)])

    # Check files deleted properly.
    for i in range(num_files):
      self.assertFalse(self.azfs.exists(file_name_pattern % i))

  @attr('AzuriteIT')
  def test_delete_files_with_errors(self):
    real_file = self.TEST_DATA_PATH + 'test_delete_files/file'
    fake_file = 'azfs://fake/fake-container/test_fake_file'
    filenames = [real_file, fake_file]
    file_size = 1024

    self._insert_random_file(real_file, file_size)

    result = self.azfs.delete_files(filenames)

    # First is the file in the real container, which shouldn't throw an error.
    self.assertEqual(result[0][0], filenames[0])
    self.assertEqual(result[0][1], None)

    # Second is the file in the fake container, which should throw a 404.
    self.assertEqual(result[1][0], filenames[1])
    self.assertEqual(result[1][1], 404)

  @attr('AzuriteIT')
  def test_list_prefix(self):
    blobs = [
        ('sloth/pictures/sleeping', 2),
        ('sloth/videos/smiling', 3),
        ('sloth/institute/costarica', 5),
    ]

    for (blob_name, size) in blobs:
      file_name = self.TEST_DATA_PATH + blob_name
      self._insert_random_file(file_name, size)

    test_cases = [
        (
            self.TEST_DATA_PATH + 's',
            [
                ('sloth/pictures/sleeping', 2),
                ('sloth/videos/smiling', 3),
                ('sloth/institute/costarica', 5),
            ]),
        (
            self.TEST_DATA_PATH + 'sloth/',
            [
                ('sloth/pictures/sleeping', 2),
                ('sloth/videos/smiling', 3),
                ('sloth/institute/costarica', 5),
            ]),
        (
            self.TEST_DATA_PATH + 'sloth/videos/smiling', [
                ('sloth/videos/smiling', 3),
            ]),
    ]

    for file_pattern, expected_object_names in test_cases:
      expected_file_names = [(self.TEST_DATA_PATH + object_name, size)
                             for (object_name, size) in expected_object_names]
      self.assertEqual(
          set(self.azfs.list_prefix(file_pattern).items()),
          set(expected_file_names))

    # Clean up.
    for (blob_name, size) in blobs:
      self.azfs.delete(self.TEST_DATA_PATH + blob_name)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
