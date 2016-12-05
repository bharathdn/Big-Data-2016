package BirdSightTraining;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Utils.FileHelper;


public class TrainingDriver {

	public static void BirdSightTrainingDriver(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "BirdSight Training");
		
//		job.addCacheFile(new Path(args[2]).toUri());

		FileHelper.deleteDir(new File(args[1]));
		
		job.setJarByClass(TrainingDriver.class);
		job.setMapperClass(TrainingMapper.TrainerMapper.class);
		job.setReducerClass(TrainingReducer.TrainerReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String args[]) throws Exception {
		BirdSightTrainingDriver(args);
	}
}
