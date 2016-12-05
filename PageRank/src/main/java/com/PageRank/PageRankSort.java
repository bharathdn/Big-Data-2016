package com.PageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Class that sorts nodes of a graph and outputs top - 100 pages 
 * in a distributed environment setup
 */
public class PageRankSort {

	public static class PageRankMapper 
	extends Mapper<Object, Text, DoubleWritable, Text> {
		static Text dummyKey = new Text("!Dummy"); 

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String linkLine = value.toString();
			String[] links = linkLine.split("\t");

			// pagerank value from incoming string
			DoubleWritable pageRankWritable = new DoubleWritable(Double.valueOf(links[1]));
			// Node id value from incoming string
			Text nodeId = new Text(links[0]);

			//			context.write(new Node(dummyKey, pageRankWritable), nodeId);
			context.write(pageRankWritable, nodeId);
		}
	}


	public static class PageRankReducer 
	extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		static int count = 0;
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {


			for (Text val : values) {
				if(count < 100) {
					context.write(val, key);
				}
				count ++;
			}
		}
	}


	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(DoubleWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleWritable rank1 = (DoubleWritable) w1;
			DoubleWritable rank2 = (DoubleWritable) w2;
			return -1 * rank1.compareTo(rank2);
		}
	}

	/*
	 * Decides which key is sent to which reducer
	 */
	public static class PageRankPartitioner extends Partitioner<DoubleWritable, Text> {
		@Override
		public int getPartition(DoubleWritable ss, Text ssValue, int numberOfPartitions) {
			// make sure that partitions are non-negative
			return 0;
		}
	}

	
	public static void PageRankSortDriver(String inputPath, String outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Page Rank Sort");
		job.setJarByClass(PageRankMgr.class);
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);
		//		job.setGroupingComparatorClass(PageRankGroupingCompatartor.class);
		job.setPartitionerClass(PageRankPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);

		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
		System.out.println("Results written to path :: "+ outputPath);
	}
}