package pageRank;

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

public class PageRankAggregator {

	public static class PageRankAggMapper 
	extends Mapper<Object, Text, NullWritable, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(NullWritable.get(), value);
		}
	}

	public static class PageRankAggReducer 
	extends Reducer<NullWritable, Text, NullWritable, Text> {

		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for(Text value : values) {
				context.write(key, value);
			}
		}
		
	}
	
	public static void prAggregatorDriver(String[] args) 
			throws Exception { 

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PageRankMatrixAgg");

		job.setNumReduceTasks(1);

		job.setJarByClass(PageRankAggregator.class);
		job.setMapperClass(PageRankAggMapper.class);
		job.setReducerClass(PageRankAggReducer.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}


	public static void main(String[] args) throws Exception {
//		Helper.deleteDir(new File(args[2]));
		prAggregatorDriver(args);
	}	
}
