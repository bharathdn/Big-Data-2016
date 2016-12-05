package com.WeatherData2.WeatherData2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/*
 * Custom writable class which will be used as value in the
 * intermediate key
 */
public class StationAttribute implements Writable{
	
	private Text type;
	private IntWritable value;
	private DoubleWritable average;
	
	public StationAttribute() {
		this.type = new Text();
		this.value = new IntWritable();
	}
	
	public StationAttribute(Text type, IntWritable value) {
		this.type = type;
		this.value = value;
		this.average = null;
	}
	
	public DoubleWritable getAverage() {
		return average;
	}

	public void setAverage(DoubleWritable average) {
		this.average = average;
	}

	public Text getType() {
		return type; 
	}
	
	public void setType(Text type) {
		this.type = type;
	}
	
	public IntWritable getValue() {
		return value;
	}
	
	public void setValue(IntWritable value) {
		this.value = value;
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		type.readFields(dataInput);
		value.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		type.write(dataOutput);
		value.write(dataOutput);
	}

	@Override
	public String toString() {
		return type + " : " + average;
	}
}
