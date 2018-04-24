package com.google.cloud.dataflow.dmesser.util;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Generate, format, and write BigQuery table row information. Use provided
 * information about the field names and types, as well as lambda functions that
 * describe how to generate their values.
 */
@SuppressWarnings("serial")
public class WriteToBigQuery<InputT> extends PTransform<PCollection<InputT>, PDone> {

	protected String projectId;
	protected String datasetId;
	protected String tableName;
	protected Map<String, FieldInfo<InputT>> fieldInfo;

	public WriteToBigQuery() {
	}

	public WriteToBigQuery(String projectId, String datasetId, String tableName,
			Map<String, FieldInfo<InputT>> fieldInfo) {
		this.projectId = projectId;
		this.datasetId = datasetId;
		this.tableName = tableName;
		this.fieldInfo = fieldInfo;
	}

	public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
			.withZone(TimeZone.getTimeZone("America/Chicago").toZoneId());

	/**
	 * A {@link Serializable} function from a {@link DoFn.ProcessContext} and
	 * {@link BoundedWindow} to the value for that field.
	 */
	public interface FieldFn<InputT> extends Serializable {
		Object apply(DoFn<InputT, TableRow>.ProcessContext context, BoundedWindow window);
	}

	public static class FieldMode implements Serializable {
		private String fieldMode = null;

		public static final FieldMode NULLABLE = new FieldMode("NULLABLE");
		public static final FieldMode REQUIRED = new FieldMode("REQUIRED");
		public static final FieldMode REPEATED = new FieldMode("REPEATED");

		private FieldMode(String fieldMode) {
			this.fieldMode = fieldMode;
		}

		public String getMode() {
			return this.fieldMode;
		}

		public String toString() {
			return this.getMode();
		}
	}

	/** Define a class to hold information about output table field definitions. */
	public static class FieldInfo<InputT> implements Serializable {
		// The BigQuery 'type' of the field
		private String fieldType;

		// The BigQuery mode for the field
		private FieldMode fieldMode;

		// A lambda function to generate the field value
		private FieldFn<InputT> fieldFn;

		public FieldInfo(String fieldType, FieldFn<InputT> fieldFn) {
			this(fieldType, FieldMode.REQUIRED, fieldFn);
		}

		public FieldInfo(String fieldType, FieldMode fieldMode, FieldFn<InputT> fieldFn) {
			this.fieldType = fieldType;
			this.fieldFn = fieldFn;
			this.fieldMode = fieldMode;
		}

		String getFieldType() {
			return this.fieldType;
		}

		FieldMode getFieldMode() {
			return this.fieldMode;
		}

		FieldFn<InputT> getFieldFn() {
			return this.fieldFn;
		}
	}

	/**
	 * Convert each key/score pair into a BigQuery TableRow as specified by fieldFn.
	 */
	protected class BuildRowFn extends DoFn<InputT, TableRow> {

		@ProcessElement
		public void processElement(ProcessContext c, BoundedWindow window) {

			TableRow row = new TableRow();
			for (Map.Entry<String, FieldInfo<InputT>> entry : fieldInfo.entrySet()) {
				String key = entry.getKey();
				FieldInfo<InputT> fcnInfo = entry.getValue();
				FieldFn<InputT> fcn = fcnInfo.getFieldFn();
				row.set(key, fcn.apply(c, window));
			}
			c.output(row);
		}
	}

	/** Build the output table schema. */
	protected TableSchema getSchema() {
		List<TableFieldSchema> fields = new ArrayList<>();
		for (Map.Entry<String, FieldInfo<InputT>> entry : fieldInfo.entrySet()) {
			String key = entry.getKey();
			FieldInfo<InputT> fcnInfo = entry.getValue();
			String bqType = fcnInfo.getFieldType();
			String bqMode = String.valueOf(fcnInfo.getFieldMode());
			fields.add(new TableFieldSchema().setName(key).setType(bqType).setMode(bqMode));
		}
		return new TableSchema().setFields(fields);
	}

	@Override
	public PDone expand(PCollection<InputT> trafficSegment) {
		trafficSegment.apply("ConvertToRow", ParDo.of(new BuildRowFn()))
				.apply(BigQueryIO.writeTableRows().to(getTable(projectId, datasetId, tableName)).withSchema(getSchema())
						.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
						.withWriteDisposition(WriteDisposition.WRITE_APPEND));
		return PDone.in(trafficSegment.getPipeline());
	}

	/** Utility to construct an output table reference. */
	static TableReference getTable(String projectId, String datasetId, String tableName) {
		TableReference table = new TableReference();
		table.setDatasetId(datasetId);
		table.setProjectId(projectId);
		table.setTableId(tableName);
		return table;
	}
}
