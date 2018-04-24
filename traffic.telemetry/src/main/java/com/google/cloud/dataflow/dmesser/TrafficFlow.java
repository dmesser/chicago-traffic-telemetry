package com.google.cloud.dataflow.dmesser;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.Pipeline;
//import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Regex;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import com.google.api.services.bigquery.model.TableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.dmesser.util.TrafficFlowOptions;
import com.google.cloud.dataflow.dmesser.util.WriteToBigQuery;

public class TrafficFlow {

	public static void main(String[] args) {

		TrafficFlowOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(TrafficFlowOptions.class);

		Pipeline p = Pipeline.create(options);

		// p.apply("ReadLines", TextIO.read().from((options.getInputFile())))
		p.apply("ReadPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
				.apply("SplitLines", Regex.split("(\\r\\n|\\r|\\n)"))
				.apply("SkipCsvHeader", Filter.by((String line) -> !line.startsWith("\"_comments\"")))
				.apply("ConvertToSegment", ParDo.of(new FormatAsTrafficSegmentFn()))
				.apply("FilterUnusedSegments", Filter.by((TrafficSegment segment) -> segment.isCurrent()))
				.apply("WindowFunction", Window.<TrafficSegment>into(FixedWindows.of(Duration.standardMinutes(1)))
						.withAllowedLateness(Duration.standardMinutes(5)).discardingFiredPanes())
				.apply("MapSegments", MapElements
						.into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptor.of(TrafficSegment.class)))
						.via((TrafficSegment segment) -> KV.of(segment.getSegmentId(), segment)))
				.apply("GroupSegments", GroupByKey.<Integer, TrafficSegment>create())
				.apply("DeduplicateSegments", ParDo.of(new DeduplicateSegmentsFn()))
				// .apply("FormatAsText", PamrDo.of(new DoFn<TrafficSegment, String>() {
				//
				// @ProcessElement
				// public void processElement(ProcessContext c) {
				// TrafficSegment segment = c.element();
				// c.output(segment.getSegmentid() + ": " + segment.getComments());
				// }
				// })).apply(TextIO.write().to(options.getOutput()).withSuffix(".txt"));
				.apply("WriteSegmentsToBigQuery",
						new WriteToBigQuery<>(options.getProject(), options.getBigQueryDataset(),
								options.getBigQueryTable(), configureGlobalWindowBigQueryWrite()));

		p.run();
	}

	@SuppressWarnings("serial")
	protected static class DeduplicateSegmentsFn extends DoFn<KV<Integer, Iterable<TrafficSegment>>, TrafficSegment> {

		@ProcessElement
		public void processElement(ProcessContext c) {
			Iterable<TrafficSegment> segments = c.element().getValue();

			Stream<TrafficSegment> segmentsStream = StreamSupport.stream(segments.spliterator(), false);
			segmentsStream.filter(distinctByKey(TrafficSegment::getLastUpdate)).forEach(c::output);
		}

		protected static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
			Set<Object> seen = ConcurrentHashMap.newKeySet();
			return t -> seen.add(keyExtractor.apply(t));
		}
	}

	/**
	 * Create a map of information that describes how to write pipeline output to
	 * BigQuery. This map is passed to the {@link WriteToBigQuery} constructor to
	 * write TrafficSegment objects.
	 */
	protected static Map<String, WriteToBigQuery.FieldInfo<TrafficSegment>> configureBigQueryWrite() {

		Map<String, WriteToBigQuery.FieldInfo<TrafficSegment>> tableConfigure = new HashMap<>();

		tableConfigure.put("segmentId",
				new WriteToBigQuery.FieldInfo<>("INTEGER", (c, w) -> c.element().getSegmentId()));
		tableConfigure.put("direction",
				new WriteToBigQuery.FieldInfo<>("STRING", (c, w) -> c.element().getDirection()));
		tableConfigure.put("fromStreet",
				new WriteToBigQuery.FieldInfo<>("STRING", (c, w) -> c.element().getFromStreet()));
		tableConfigure.put("lastUpdate", new WriteToBigQuery.FieldInfo<>("TIMESTAMP",
				(c, w) -> c.element().getLastUpdate().format(WriteToBigQuery.DATE_TIME_FORMATTER)));
		tableConfigure.put("startLatitude",
				new WriteToBigQuery.FieldInfo<>("FLOAT", (c, w) -> c.element().getStartLatitude()));
		tableConfigure.put("endLatitude",
				new WriteToBigQuery.FieldInfo<>("FLOAT", (c, w) -> c.element().getEndLatitude()));
		tableConfigure.put("endLongitude",
				new WriteToBigQuery.FieldInfo<>("FLOAT", (c, w) -> c.element().getEndLongitude()));
		tableConfigure.put("streetHeading",
				new WriteToBigQuery.FieldInfo<>("STRING", (c, w) -> c.element().getStreetHeading()));
		tableConfigure.put("toStreet", 
				new WriteToBigQuery.FieldInfo<>("STRING", (c, w) -> c.element().getToStreet()));
		tableConfigure.put("speed", 
				new WriteToBigQuery.FieldInfo<>("INTEGER", (c, w) -> c.element().getSpeed()));
		tableConfigure.put("traffic",
				new WriteToBigQuery.FieldInfo<>("STRING", (c, w) -> c.element().getTraffic()));
		tableConfigure.put("comments", 
				new WriteToBigQuery.FieldInfo<>("STRING", WriteToBigQuery.FieldMode.NULLABLE,
				(c, w) -> c.element().getComments()));
		tableConfigure.put("startLongitude",
				new WriteToBigQuery.FieldInfo<>("FLOAT", (c, w) -> c.element().getStartLongitude()));
		tableConfigure.put("street", 
				new WriteToBigQuery.FieldInfo<>("STRING", (c, w) -> c.element().getStreet()));

		return tableConfigure;
	}

	/**
	 * Create a map of information that describes how to write pipeline output to
	 * BigQuery. This map is used to write traffic segments and adds a processing
	 * time field.
	 */
	protected static Map<String, WriteToBigQuery.FieldInfo<TrafficSegment>> configureGlobalWindowBigQueryWrite() {

		Map<String, WriteToBigQuery.FieldInfo<TrafficSegment>> tableConfigure = configureBigQueryWrite();

		tableConfigure.put("processingTime", new WriteToBigQuery.FieldInfo<>("TIMESTAMP",
				(c, w) -> LocalDateTime.now().format(WriteToBigQuery.DATE_TIME_FORMATTER)));

		return tableConfigure;
	}

	/**
	 * Function class to process a text line of comma-separated values in quotes and
	 * turn it into a TrafficSegment object.
	 */
	@SuppressWarnings("serial")
	static class FormatAsTrafficSegmentFn extends DoFn<String, TrafficSegment> {
		private static final Logger LOG = LoggerFactory.getLogger(FormatAsTrafficSegmentFn.class);

		@ProcessElement
		public void processElement(ProcessContext c) {
			String line = c.element();
			String[] components = line.split(",");
			try {
				String comments = components[0].trim().replace("\"", "");
				String direction = components[1].trim().replace("\"", "");
				String fromStreet = components[2].trim().replace("\"", "");
				String lastUpdate = components[3].trim().replace("\"", "");
				String length = components[4].trim().replace("\"", "");
				String startLatitude = components[5].trim().replace("\"", "");
				String endLatitude = components[6].trim().replace("\"", "");
				String endLongitude = components[7].trim().replace("\"", "");
				String streetHeading = components[8].trim().replace("\"", "");
				String toStreet = components[9].trim().replace("\"", "");
				String speed = components[10].trim().replace("\"", "");
				String segmentId = components[11].trim().replace("\"", "");
				String startLongitude = components[12].trim().replace("\"", "");
				String street = components[13].trim().replace("\"", "");

				TrafficSegment segment = new TrafficSegment();

				segment.setComments(comments);
				segment.setDirection(direction);
				segment.setFromStreet(fromStreet);
				segment.setLastUpdate(lastUpdate);
				segment.setLength(length);
				segment.setStartLatitude(startLatitude);
				segment.setEndLatitude(endLatitude);
				segment.setEndLongitude(endLongitude);
				segment.setStreetHeading(streetHeading);
				segment.setToStreet(toStreet);
				segment.setSpeed(speed);
				segment.setSegmentid(segmentId);
				segment.setStartLongitude(startLongitude);
				segment.setStreet(street);

				c.output(segment);

			} catch (ArrayIndexOutOfBoundsException | NumberFormatException | DateTimeParseException e) {
				LOG.warn("Parse error on " + line + ", " + e.getMessage());
			}
		}
	}

}
