package com.google.cloud.dataflow.dmesser;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

//import org.apache.avro.reflect.Nullable;
//import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;


@DefaultCoder(SerializableCoder.class)
public class TrafficSegment implements Serializable{
	
	private static final long serialVersionUID = -6676197647307952052L;
	public static final String HEAVY_TRAFFIC = "heavy";
	public static final String MEDIUM_TRAFFIC = "medium";
	public static final String FREE_FLOW_TRAFFIC = "free";
	
	private int segmentid = 0;
	private String street = null;
	private String _direction = null;
	private String _fromst = null;
	private String _tost = null;
	private float _length = Float.NaN;
	private String _strheading = null;
	private String _comments = null;
	private double start_lon = Double.NaN;
	private double _lif_lat = Double.NaN;
	private double _lit_lon = Double.NaN;
	private double _lit_lat = Double.NaN;
	private int _traffic = -2;
	private LocalDateTime _last_updt = null;

	public int getSegmentId() throws IllegalStateException {
		if (this.segmentid != 0)
			return segmentid;
		else
			throw new IllegalStateException("segment id unitialized");
	}

	public String getStreet() {
		return street;
	}

	public String getDirection() {
		return _direction;
	}

	public String getFromStreet() {
		return _fromst;
	}

	public String getToStreet() {
		return _tost;
	}

	public float getLength() {
		return _length;
	}

	public String getStreetHeading() {
		return _strheading;
	}

	public String getComments() {
		return _comments;
	}

	public double getStartLongitude() {
		return start_lon;
	}

	public double getStartLatitude() {
		return _lif_lat;
	}

	public double getEndLongitude() {
		return _lit_lon;
	}

	public double getEndLatitude() {
		return _lit_lat;
	}

	public int getSpeed() {
		return _traffic;
	}

	public LocalDateTime getLastUpdate() throws IllegalStateException {
		if (this._last_updt != null)
			return _last_updt;
		else
			throw new IllegalStateException("last update timestamp not set");
	}

	public void setSegmentid(String segmentid) throws NumberFormatException {
		this.segmentid = Integer.parseInt(segmentid);
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public void setDirection(String direction) {
		this._direction = direction;
	}

	public void setFromStreet(String fromStreet) {
		this._fromst = fromStreet;
	}

	public void setToStreet(String toStreet) {
		this._tost = toStreet;
	}

	public void setLength(String length) throws NumberFormatException {
		this._length = Float.parseFloat(length);
	}

	public void setStreetHeading(String streetHeading) {
		this._strheading = streetHeading;
	}

	public void setComments(String comments) {
		this._comments = comments;
	}

	public void setStartLongitude(String startLongitude) throws NumberFormatException {
		this.start_lon = Double.parseDouble(startLongitude);
	}

	public void setStartLatitude(String startLatitude) throws NumberFormatException {
		this._lif_lat = Double.parseDouble(startLatitude);
	}

	public void setEndLongitude(String endLongitude) throws NumberFormatException {
		this._lit_lon = Double.parseDouble(endLongitude);
	}

	public void setEndLatitude(String endLatitude) throws NumberFormatException {
		this._lit_lat = Double.parseDouble(endLatitude);
	}

	public void setSpeed(String speed) throws NumberFormatException {
		this._traffic = Integer.parseInt(speed);
	}

	public void setLastUpdate(String lastUpdate) throws DateTimeParseException{
		// SimpleDateFormat formatter = new SimpleDateFormat("2018-04-14 19:50:19.0");
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
		this._last_updt = LocalDateTime.parse(lastUpdate, formatter);
	}

	public boolean isUnused() {
		return this.getSpeed() == -1 && this.getLastUpdate().isBefore(LocalDateTime.now().minusDays(3));
	}
	
	public boolean isCurrent() {
		return !this.isUnused();
	}
	
	public String getTraffic() throws IllegalStateException {
		if(this.getSpeed() != -2) {

			if(this.getSpeed() >=0 && this.getSpeed() <= 9)
				return TrafficSegment.HEAVY_TRAFFIC;
			else if (this.getSpeed() >= 10 && this.getSpeed() <= 20)
				return TrafficSegment.MEDIUM_TRAFFIC;
			else
				return TrafficSegment.FREE_FLOW_TRAFFIC;

		} else {
			throw new IllegalStateException("no traffic data set.");
		}
	}
}
