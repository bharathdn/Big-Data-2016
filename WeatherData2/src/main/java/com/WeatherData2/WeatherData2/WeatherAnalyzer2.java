package com.WeatherData2.WeatherData2;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAnalyzer2 {

	/*
	 * CONSTANTS
	 */
	private static final String TMIN = "TMIN";
	private static final String TMAX = "TMAX";

	// index value of Temperature statistics in csv
	private static final int STNIDINDEX = 0;
	private static final int DATEINDEX = 1;
	private static final int TEMPKEYINDEX = 2;
	private static final int TEMPVALUEINDEX = 3;
	private static final int NUMBEROFYEARDIGITS = 4;
	private static IntWritable one = new IntWritable(1);

	/*
	 * Mapper class for emitting temperature values for a given <stationID, Year> key
	 * 
	 *  It uses in-mapper combining for optimization which reduces the number of records that 
	 *  need to be processed by a reducer 
	 *  
	 *  Further, the year value is used as a composite key in combination with StationId
	 *  so that Hadoop sorts the incoming values to a reducer bases on ascending value of year
	 */
	public static class TemperatureMapper extends Mapper<Object, Text, StationStatisticsKey, StationStatisticsValue> {

		Map<StationStatisticsKey, StationStatisticsValue> map;

		// value: line from the file
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			if(line.contains(TMAX) || line.contains(TMIN)) {

				String[] valueArray = value.toString().split(",");
				int tempValue = Integer.parseInt(valueArray[TEMPVALUEINDEX]);
				Text stationId = new Text(valueArray[STNIDINDEX]);
				int yearInt = Integer.parseInt(valueArray[DATEINDEX].substring(0, NUMBEROFYEARDIGITS));
				IntWritable year = new IntWritable(yearInt);

				StationStatisticsKey ssKey = new StationStatisticsKey();
				ssKey.setStationId(stationId);
				ssKey.setYear(year);

				StationStatisticsValue ssValue;
				
				if(map.containsKey(ssKey)) {
					ssValue = map.get(ssKey);
					if(valueArray[TEMPKEYINDEX].equals(TMAX)) {
						ssValue.settMaxSum(new IntWritable(tempValue));
						ssValue.settMaxCount(one);
					}
					else {
						ssValue.settMinSum(new IntWritable(tempValue));
						ssValue.settMinCount(one);
					}
				}
				else {
					
					ssValue = new StationStatisticsValue();
					ssValue.setYear(year);
					
					if(valueArray[TEMPKEYINDEX].equals(TMAX)) {
						ssValue.settMaxSum(new IntWritable(tempValue + ssValue.gettMaxSum().get()));
						ssValue.settMaxCount(new IntWritable(1 + ssValue.gettMaxCount().get()));
					}
					else {
						ssValue.settMinSum(new IntWritable(tempValue + ssValue.gettMinSum().get()));
						ssValue.settMinCount(new IntWritable(1 + ssValue.gettMinCount().get()));
					}
				}
				map.put(ssKey, ssValue);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			map = new HashMap<StationStatisticsKey, StationStatisticsValue>();
		}


		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for(Map.Entry<StationStatisticsKey, StationStatisticsValue> entry : map.entrySet()) {
				context.write(entry.getKey(), entry.getValue());
			}
		}

	}

	/*
	 * Input 
	 * key: composite key<stationId, Year>
	 * values: Year and List of objects containing Temperature values (objects of type StationStatisticsValue)
	 * 
	 * For each reduce call
	 * For each key: The order of values coming are sorted by year. 
	 * EG: for a single call, the keys can be 
	 * 	<StnA, 1990> : Value= <1990, maxTempSum, minTempSum ... >
	 * 	<StnA, 1990> : Value= <1990, maxTempSum, minTempSum ... >
	 * 	<StnA, 1990> : Value= <1990, maxTempSum, minTempSum ... >
	 * 	<StnA, 1991> : Value= <1991, maxTempSum, minTempSum ... >
	 * 	<StnA, 1991> : Value= <1991, maxTempSum, minTempSum ... >
	 * 	<StnA, 1995> : Value= <1995, maxTempSum, minTempSum ... >
	 * 
	 *   where the station Id is same and Values are sorted by year,
	 *   A same year can appear more than once because, it might be for a different month/day
	 */
	public static class TemperatureReducer
	extends Reducer<StationStatisticsKey, StationStatisticsValue, StationStatisticsKey, Text> {

		public void reduce(StationStatisticsKey key, Iterable<StationStatisticsValue> values, Context context) 
				throws IOException, InterruptedException {

			StationStatisticsValue result = new StationStatisticsValue();			
			IntWritable prevYear = null;
			StringBuilder sb = new StringBuilder();
			sb.append("[");

			for (StationStatisticsValue val : values) {
				if(prevYear == null) { 
					prevYear = new IntWritable(val.getYear().get()); 
				}

				IntWritable currYear = val.getYear();
				if(currYear.get() != prevYear.get()) {

					double tMaxAverage = (double) result.gettMaxSum().get() / result.gettMaxCount().get();
					result.settMaxAverage(new DoubleWritable(tMaxAverage));
					double tMinAverage = (double) result.gettMinSum().get() / result.gettMinCount().get();
					result.settMinAverage(new DoubleWritable(tMinAverage));
					sb = sb.append(appendResult(result, prevYear.toString()));


					result = new StationStatisticsValue();
					prevYear = new IntWritable(val.getYear().get());			
				}

				result.setYear(currYear);
				result.settMaxSum(new IntWritable(result.gettMaxSum().get() + val.gettMaxSum().get()));
				result.settMaxCount(new IntWritable(result.gettMaxCount().get() + val.gettMaxCount().get()));
				result.settMinSum(new IntWritable(result.gettMinSum().get() + val.gettMinSum().get()));
				result.settMinCount(new IntWritable(result.gettMinCount().get() + val.gettMinCount().get()));
			}


			// This code is repetitive, but is necessary to include the data for the year that appears last
			double tMaxAverage = (double) result.gettMaxSum().get() / result.gettMaxCount().get();
			result.settMaxAverage(new DoubleWritable(tMaxAverage));
			double tMinAverage = (double) result.gettMinSum().get() / result.gettMinCount().get();
			result.settMinAverage(new DoubleWritable(tMinAverage));
			sb.append(appendResult(result, prevYear.toString()));
			sb.deleteCharAt(sb.length() - 1);
			sb.append("]");

			Text finalResult = new Text(sb.toString());
			context.write(key, finalResult);
		}

		private String appendResult(StationStatisticsValue value, String year) {
			StringBuilder sb = new StringBuilder();
			sb.append("(").append(year).append(", ").
			append(value.gettMaxAverage()).append(", ").
			append(value.gettMinAverage()).append(")").append(",");
			return sb.toString();
		} 
	}


	/*
	 * Decides which key is sent to which reducer
	 */
	public static class TemperaturePartitioner extends Partitioner<StationStatisticsKey, StationStatisticsValue> {
		@Override
		public int getPartition(StationStatisticsKey ss, StationStatisticsValue ssValue, int numberOfPartitions) {
			// make sure that partitions are non-negative
			return Math.abs(ss.getStationId().hashCode() % numberOfPartitions);
		}
	}


	public static class TemperatureGroupingCompatartor extends WritableComparator {

		public TemperatureGroupingCompatartor() {
			super(StationStatisticsKey.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable wss1, WritableComparable wss2) {
			StationStatisticsKey ss1 = (StationStatisticsKey) wss1;
			StationStatisticsKey ss2 = (StationStatisticsKey) wss2;

			int compareValue = ss1.getStationId().compareTo(ss2.getStationId());
			return compareValue; 
		}
	}

	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(StationStatisticsKey.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			StationStatisticsKey ss1 = (StationStatisticsKey) w1;
			StationStatisticsKey ss2 = (StationStatisticsKey) w2;
			int cmp = ss1.getStationId().compareTo(ss2.getStationId());
			if (cmp != 0) {
				return cmp;
			}
			return ss1.getYear().compareTo(ss2.getYear());
		}
	}


	public static void WeatherAnalyzer2Driver(String[] args) throws Exception{
		Configuration conf = new Configuration();

//		App.deleteDir(new File(args[1]));
		Job job = Job.getInstance(conf, "Weather Time Series Computation");
		job.setJarByClass(WeatherAnalyzer1A.class);
		job.setMapperClass(TemperatureMapper.class);
		job.setReducerClass(TemperatureReducer.class);
		job.setPartitionerClass(TemperaturePartitioner.class);
		job.setGroupingComparatorClass(TemperatureGroupingCompatartor.class);

		job.setMapOutputKeyClass(StationStatisticsKey.class);
		job.setMapOutputValueClass(StationStatisticsValue.class);

		job.setOutputKeyClass(StationStatisticsKey.class);
		job.setOutputValueClass(StationStatisticsValue.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	// driver defined in main for debugging on IDE, please ignore
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
//		App.deleteDir(new File(args[1]));
		Job job = Job.getInstance(conf, "Weather Time Series Computation");
		job.setJarByClass(WeatherAnalyzer1A.class);
		job.setMapperClass(TemperatureMapper.class);
		job.setReducerClass(TemperatureReducer.class);
		job.setPartitionerClass(TemperaturePartitioner.class);
		job.setGroupingComparatorClass(TemperatureGroupingCompatartor.class);

		job.setMapOutputKeyClass(StationStatisticsKey.class);
		job.setMapOutputValueClass(StationStatisticsValue.class);

		job.setOutputKeyClass(StationStatisticsKey.class);
		job.setOutputValueClass(StationStatisticsValue.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}