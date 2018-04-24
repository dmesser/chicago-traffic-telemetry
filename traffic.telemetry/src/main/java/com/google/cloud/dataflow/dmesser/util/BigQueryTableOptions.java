package com.google.cloud.dataflow.dmesser.util;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

import com.google.api.services.bigquery.model.TableSchema;

public interface BigQueryTableOptions extends GcpOptions {
	@Description("BigQuery dataset name")
	@Default.String("beam_examples")
	String getBigQueryDataset();

	void setBigQueryDataset(String dataset);

	@Description("BigQuery table name")
	@Default.InstanceFactory(BigQueryTableFactory.class)
	String getBigQueryTable();

	void setBigQueryTable(String table);

	@Description("BigQuery table schema")
	TableSchema getBigQuerySchema();

	void setBigQuerySchema(TableSchema schema);

	/**
	 * Returns the job name as the default BigQuery table name.
	 */
	class BigQueryTableFactory implements DefaultValueFactory<String> {
		@Override
		public String create(PipelineOptions options) {
			return options.getJobName().replace('-', '_');
		}
	}
}
