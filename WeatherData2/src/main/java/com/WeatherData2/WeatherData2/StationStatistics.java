package com.WeatherData2.WeatherData2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StationStatistics implements Writable{
	private Text stationId;
	
	private IntWritable tMaxSum;
	private IntWritable tMaxCount;
	private DoubleWritable tMaxAverage;
	
	private IntWritable tMinSum;
	private IntWritable tMinCount;
	private DoubleWritable tMinAverage;

	
	public StationStatistics() {
		this.stationId = new Text();
		
		this.tMaxCount = new IntWritable(0);
		this.tMaxSum = new IntWritable(0);
		this.tMaxAverage = new DoubleWritable(0);
		
		this.tMinCount = new IntWritable(0);
		this.tMinSum = new IntWritable(0);
		this.tMinAverage = new DoubleWritable(0);
	}
	
	public Text getStationId() {
		return stationId;
	}

	public void setStationId(Text stationId) {
		this.stationId = stationId;
	}

	public IntWritable gettMaxSum() {
		return tMaxSum;
	}

	public void settMaxSum(IntWritable tMaxSum) {
		this.tMaxSum = tMaxSum;
	}

	public IntWritable gettMaxCount() {
		return tMaxCount;
	}

	public void settMaxCount(IntWritable tMaxCount) {
		this.tMaxCount = tMaxCount;
	}

	public DoubleWritable gettMaxAverage() {
		return tMaxAverage;
	}

	public void settMaxAverage(DoubleWritable tMaxAverage) {
		this.tMaxAverage = tMaxAverage;
	}

	public IntWritable gettMinSum() {
		return tMinSum;
	}

	public void settMinSum(IntWritable tMinSum) {
		this.tMinSum = tMinSum;
	}

	public IntWritable gettMinCount() {
		return tMinCount;
	}

	public void settMinCount(IntWritable tMinCount) {
		this.tMinCount = tMinCount;
	}

	public DoubleWritable gettMinAverage() {
		return tMinAverage;
	}

	public void settMinAverage(DoubleWritable tMinAverage) {
		this.tMinAverage = tMinAverage;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		stationId.readFields(dataInput);
		
		tMaxSum.readFields(dataInput);
		tMaxCount.readFields(dataInput);
		tMaxAverage.readFields(dataInput);
		
		tMinSum.readFields(dataInput);
		tMinCount.readFields(dataInput);
		tMinAverage.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		stationId.write(dataOutput);
		
		tMaxSum.write(dataOutput);
		tMaxCount.write(dataOutput);
		tMaxAverage.write(dataOutput);
		
		tMinSum.write(dataOutput);
		tMinCount.write(dataOutput);
		tMinAverage.write(dataOutput);
	}	

	@Override
	public String toString() {
		return ", " + tMaxAverage + ", "+ tMinAverage;
	}
}
