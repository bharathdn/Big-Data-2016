package com.WeatherData2.WeatherData2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class StationStatisticsKey  implements Writable, WritableComparable<StationStatisticsKey> {
	private Text stationId;
	private IntWritable year;

	public StationStatisticsKey() {
		this.stationId = new Text();
		this.year = new IntWritable();
	}

	public Text getStationId() {
		return stationId;
	}

	public void setStationId(Text stationId) {
		this.stationId = stationId;
	}	

	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = year;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		stationId.readFields(dataInput);
		year.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		stationId.write(dataOutput);
		year.write(dataOutput);
	}	

	@Override
	public int compareTo(StationStatisticsKey ss) {
		int compareValue = this.stationId.compareTo(ss.stationId);
		if(compareValue == 0) {
			compareValue = this.year.compareTo(ss.year);
		}
		return compareValue;
	}

	@Override
	public String toString() {
		return stationId + ",";
	}
}