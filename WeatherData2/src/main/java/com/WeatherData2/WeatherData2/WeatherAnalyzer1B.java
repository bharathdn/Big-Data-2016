package com.WeatherData2.WeatherData2;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Separate Combiner approach
 */
public class WeatherAnalyzer1B {

	/*
	 * CONSTANTS
	 */
	private static final String TMIN = "TMIN";
	private static final String TMAX = "TMAX";

	// index value of Temperature statistics in csv
	private static final int STNIDINDEX = 0;
	private static final int TEMPKEYINDEX = 2;
	private static final int TEMPVALUEINDEX = 3;
	private static IntWritable one = new IntWritable(1); 

	public static class TemperatureMapper 
	extends Mapper<Object, Text, Text, StationStatistics> {

		// value: line from the file
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			if(line.contains(TMAX) || line.contains(TMIN)) {
				String[] valueArray = value.toString().split(",");
				StationStatistics sb = new StationStatistics();
				IntWritable tempValue = new IntWritable(Integer.parseInt(valueArray[TEMPVALUEINDEX]));
				if(valueArray[TEMPKEYINDEX].equals(TMAX)) {
					sb.settMaxSum(tempValue);
					sb.settMaxCount(one);
				}
				else {
					sb.settMinSum(tempValue);
					sb.settMinCount(one);
				}
				context.write(new Text(valueArray[STNIDINDEX]), sb);
			}
		}
	}


	/*
	 * Combiner class
	 */
	public static class TemperatureCombiner
	extends Reducer<Text, StationStatistics, Text, StationStatistics> {

		public void reduce(Text key, Iterable<StationStatistics> values, Context context) 
				throws IOException, InterruptedException {

			StationStatistics ss = new StationStatistics();
			ss.setStationId(key);
			for (StationStatistics val : values) {				
				ss.settMaxSum(new IntWritable(ss.gettMaxSum().get() + val.gettMaxSum().get()));
				ss.settMaxCount(new IntWritable(ss.gettMaxCount().get() + val.gettMaxCount().get()));
				ss.settMinSum(new IntWritable(ss.gettMinSum().get() + val.gettMinSum().get()));
				ss.settMinCount(new IntWritable(ss.gettMinCount().get() + val.gettMinCount().get()));
			}
			context.write(key, ss);	
		}
	}


	public static class TemperatureReducer
	extends Reducer<Text, StationStatistics, Text, StationStatistics> { 

		public void reduce(Text key, Iterable<StationStatistics> values, Context context) 
				throws IOException, InterruptedException {
			StationStatistics ss = new StationStatistics();
			ss.setStationId(key);

			for (StationStatistics val : values) {	
				ss.settMaxSum(new IntWritable(ss.gettMaxSum().get() + val.gettMaxSum().get()));
				ss.settMaxCount(new IntWritable(ss.gettMaxCount().get() + val.gettMaxCount().get()));
				ss.settMinSum(new IntWritable(ss.gettMinSum().get() + val.gettMinSum().get()));
				ss.settMinCount(new IntWritable(ss.gettMinCount().get() + val.gettMinCount().get()));
			}

			double tMaxAverage = (double) ss.gettMaxSum().get() / ss.gettMaxCount().get();
			ss.settMaxAverage(new DoubleWritable(tMaxAverage));
			double tMinAverage = (double) ss.gettMinSum().get() / ss.gettMinCount().get();
			ss.settMinAverage(new DoubleWritable(tMinAverage));
			context.write(key, ss);
		}
	}

	
	public static void WeatherAnalyzer1BDriver (String[] args) throws Exception {
		Configuration conf = new Configuration();

//		App.deleteDir(new File(args[1]));
		Job job = Job.getInstance(conf, "Weather Mean Computation 1B");
		job.setJarByClass(WeatherAnalyzer1B.class);
		job.setMapperClass(TemperatureMapper.class);
		job.setCombinerClass(TemperatureCombiner.class);
		job.setReducerClass(TemperatureReducer.class);

		// mapper o/p
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StationStatistics.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StationStatistics.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
