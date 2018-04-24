package com.google.cloud.dataflow.dmesser.util;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;

public interface TrafficFlowOptions extends BigQueryTableOptions{

	@Description("Path of the file to read from")
	@Default.String("gs://dmesser-traffic-telemetry/chicago-traffic-congestion-2018-04-12.json")
	String getInputFile();

	void setInputFile(String value);

	/**
	 * Set this required option to specify where to write the output.
	 */
	@Description("Path of the file to write to")
	@Required
	String getOutput();

	void setOutput(String value);
	
	
	/**
	 * Set this required option to specify from which PubSub topic to read.
	 */
	@Description("A PubSub Topic to read from")
	@Required
	String getTopic();

	void setTopic(String value);
}
