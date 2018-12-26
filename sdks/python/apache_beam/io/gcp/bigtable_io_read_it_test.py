import logging
import unittest
import datetime
import uuid

import apache_beam as beam
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from apache_beam.runners.runner import PipelineState
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.pipeline_options import PipelineOptions
from bigtable import ReadBigtableOptions,BigtableReadConfiguration,ReadFromBigtable

class BigtableIOReadIT(unittest.TestCase):
	DEFAULT_TABLE_PREFIX = "python"
	
	PROJECT_NAME = "grass-clump-479"
	INSTANCE_NAME = "python-write"
	TABLE_NAME = DEFAULT_TABLE_PREFIX + "-" + str(uuid.uuid4())[:8]
	number = 500

	def setUp(self):
		argv = [ '--test-pipeline-options="--runner=DirectRunner"' ]
		self.test_pipeline = TestPipeline(is_integration_test=True,argv=argv)
		self.runner_name = type(self.test_pipeline.runner).__name__
		self.project = self.PROJECT_NAME

		client = bigtable.Client(project=self.project, admin=True)

		instance = client.instance(self.INSTANCE_NAME)
		self.table = instance.table(self.TABLE_NAME)

		self._write_rows(self.number)

	def tearDown(self):
		if self.table.exists():
			self.table.delete()

	@attr('IT')
	def test_bigtable_read_python(self):
		output = self.TABLE_NAME
		number = self.number
		
		config = BigtableReadConfiguration(self.project, self.INSTANCE_NAME, self.TABLE_NAME)
		
		pipeline_args = self.test_pipeline.options_list

		pipeline_options = PipelineOptions(pipeline_args)

		read_from_bigtable = ReadFromBigtable(config)
		with beam.Pipeline(options=pipeline_options) as p: 
			counts = (p 
			| 'read' >> beam.io.Read(read_from_bigtable))
		
			result = p.run()
			result.wait_until_finish()

			assert result.state == PipelineState.DONE

			if (not hasattr(result, 'has_job') or result.has_job):
				read_filter = MetricsFilter().with_name('read')
				query_result = result.metrics().query(read_filter)
				if query_result['counters']:
					read_counter = query_result['counters'][0]

					logging.info('Number of Rows: %d', read_counter.committed)
					assert read_counter.committed == number


	def _write_rows(self,number):
		table = self.table
		

		max_versions_rule = column_family.MaxVersionsGCRule(2)
		column_family_id = 'cf1'
		column_families = {column_family_id: max_versions_rule}

		column = column_family_id
		if not table.exists():
			self.table.create(column_families=column_families)

		rows = []
		for i, value in enumerate(range(number)):
			row_key = 'row-{}'.format(i).encode()
			row = table.row(row_key)
			row.set_cell(column_family_id,
				column,
				value,
				timestamp=datetime.datetime.utcnow())
			rows.append(row)
		table.mutate_rows(rows)
		

if __name__ == '__main__':
	logging.getLogger().setLevel(logging.INFO)
	unittest.main()