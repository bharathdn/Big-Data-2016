package BirdSightTraining;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Utils.FileHelper;

public class TrainerEx {


	public static class TrainerMapper 
	extends Mapper<Object, Text, NullWritable, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();	
			String[] lineSplit = line.split(",");

			StringBuilder sb = new StringBuilder();
			for(int i = 0 ; i < 27 ; i++){
				sb.append(lineSplit[i]).append(", ");
			}

			sb.deleteCharAt(sb.length() - 1);
			sb.deleteCharAt(sb.length() - 1);
			context.write(NullWritable.get(), new Text(sb.toString()));
		}
	}


	public static class TrainerReducer 
	extends Reducer<Object, Text, NullWritable, Text> {

		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for(Text value : values) {
				if(count++ < 2000) {
					context.write(key, value);
				}
			}
		}
	}


	public static void BirdSightTrainingDriver(String[] args) throws Exception {
		Configuration conf = new Configuration();

		FileHelper.deleteDir(new File(args[1]));
		Job job = Job.getInstance(conf, "BirdSight Sample collection");
		job.setJarByClass(TrainingDriver.class);
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
