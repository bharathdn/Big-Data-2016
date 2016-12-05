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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 * In-Mapper Combiner approach
 */

public class WeatherAnalyzer1C {

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

		Map<Text, StationStatistics> map; // = new HashMap<Text, StationStatistics>(); 

		// value: line from the file
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			if(line.contains(TMAX) || line.contains(TMIN)) {
				String[] valueArray = value.toString().split(",");
				Text stationId = new Text(valueArray[STNIDINDEX]);
				int tempValue = Integer.parseInt(valueArray[TEMPVALUEINDEX]);

				StationStatistics sb;
				if(map.containsKey(stationId)) {
					sb = map.get(stationId);
					if(valueArray[TEMPKEYINDEX].equals(TMAX)){
						sb.settMaxSum(new IntWritable(sb.gettMaxSum().get() + tempValue));
						sb.settMaxCount(new IntWritable(sb.gettMaxCount().get() + 1));
					}
					else{
						sb.settMinSum(new IntWritable(sb.gettMinSum().get() + tempValue));
						sb.settMinCount(new IntWritable(sb.gettMinCount().get() + 1));
					}
				}
				else {
					sb = new StationStatistics();
					if(valueArray[TEMPKEYINDEX].equals(TMAX)){
						sb.settMaxSum(new IntWritable(tempValue));
						sb.settMaxCount(one);
					}
					else{
						sb.settMinSum(new IntWritable(tempValue));
						sb.settMinCount(one);
					}
				}
				map.put(stationId, sb);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			map = new HashMap<Text, StationStatistics>();
		}


		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for(Map.Entry<Text, StationStatistics> entry : map.entrySet()) {
				context.write(entry.getKey(), entry.getValue());
			}
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


	public static void WeatherAnalyzer1CDriver(String[] args) throws Exception {
		Configuration conf = new Configuration();

//		App.deleteDir(new File(args[1]));
		Job job = Job.getInstance(conf, "Weather Mean Computation 1C");
		job.setJarByClass(WeatherAnalyzer1A.class);
		job.setMapperClass(TemperatureMapper.class);
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
