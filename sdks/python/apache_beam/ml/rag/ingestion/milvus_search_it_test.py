import platform
import unittest
import pytest


@pytest.mark.require_docker_in_docker
@unittest.skipUnless(
    platform.system() == "Linux",
    "Test runs only on Linux due to lack of support, as yet, for nested "
    "virtualization in CI environments on Windows/macOS. Many CI providers run "
    "tests in virtualized environments, and nested virtualization "
    "(Docker inside a VM) is either unavailable or has several issues on "
    "non-Linux platforms.")
class TestMilvusVectorWriterConfig(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    pass

  @classmethod
  def tearDownClass(cls):
    pass

  def test_invalid_write_on_non_existent_collection(self):
    pass

  def test_invalid_write_on_non_existent_partition(self):
    pass

  def test_invalid_write_on_non_existent_field(self):
    pass

  def test_invalid_write_on_missing_primary_key_in_entity(self):
    pass

  def test_write_on_existent_collection(self):
    pass
  
  def test_write_on_existent_partition(self):
    pass

  def test_write_on_auto_id_primary_key_in_entity(self):
    # New Record
    pass
  
  def test_write_on_non_auto_id_primary_key_in_entity(self):
    pass

  def tets_write_on_default_schema(self):
    pass

  def test_write_on_custom_column_specifications(self):
    pass