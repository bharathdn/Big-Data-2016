package PreProcessing;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Utils.Constants;
import Utils.FileHelper;

public class fetchFeatureValues {
	
	public static class TrainerMapper 
	extends Mapper<Object, Text, NullWritable, Text> {
		Set<String> feature;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			feature = new HashSet<String>();
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] lineSplit = line.split(",");
			feature.add(lineSplit[Constants.COUNTY_INDEX]);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			
			for(String state : feature) {
				context.write(NullWritable.get(), new Text(state));
			}
		}
	}
	
	
	public static class TrainerReducer 
	extends Reducer<Object, Text, NullWritable, Text> {
		Set<String> feature;

		public void reduce(Object key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			for(Text state : values) {
				feature.add(state.toString());
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			feature = new HashSet<String>();
		}


		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			
			for(String state : feature) {
				context.write(NullWritable.get(), new Text(state));
			}
		}
	}
	
	public static void BirdSightTrainingDriver(String[] args) throws Exception {
		Configuration conf = new Configuration();

		FileHelper.deleteDir(new File(args[1]));
		Job job = Job.getInstance(conf, "BirdSight Fature values fetching");
		job.setJarByClass(fetchFeatureValues.class);
		job.setMapperClass(TrainerMapper.class);
		job.setReducerClass(TrainerReducer.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1); 
	}
	
	public static void main(String args[]) throws Exception {
		BirdSightTrainingDriver(args);
	}

}
