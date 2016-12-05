package parserB;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Alias {

	// Global counter to set link count
	static enum UpdateCount {
		LinkCount
	}

	/*
	 * Mapper for Matrix 
	 */
	public static class NodeMapper 
	extends Mapper<Object, Text, Text, Text> {

		private static Text DUMMY = new Text("!DUMMY");

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// value => { parentNodeId	adjList }
			// eg: { A	=> 	B,C,D
			String[] textSplit = value.toString().split("\t");
			context.write(DUMMY ,new Text(textSplit[0]));
		}
	}

	/*
	 * REDUCE CLASS
	 */
	public static class NodeReducer 
	extends Reducer<Text, Text, LongWritable, Text> {

		Map<Long, String> nodeMap = new HashMap<Long, String>();
		private static long linkCount = 0;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for(Text nodeId : values) {
				nodeMap.put(linkCount++, nodeId.toString());
			}

			for(Map.Entry<Long, String> entrySet : nodeMap.entrySet()) {
				context.write(new LongWritable(entrySet.getKey()), new Text(entrySet.getValue()));
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("Number of Nodes ::" + nodeMap.size());
			long linksCount = nodeMap.size();
			context.getCounter(UpdateCount.LinkCount).increment(linksCount);
		}	
	}


	/*
	 * DRIVER
	 */
	public static long NodeAliasDriver(String[] args) 
			throws Exception { 

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PageRankMatrixAliaser");
		
		job.setNumReduceTasks(1);
		
		job.setJarByClass(Alias.class);
		job.setMapperClass(NodeMapper.class);
		job.setReducerClass(NodeReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		long linkCount = job.getCounters().findCounter(UpdateCount.LinkCount).getValue();
		return linkCount;
	}

	public static long main(String[] args) throws Exception {
		//		Helper.deleteDir(new File(args[1]));
		return NodeAliasDriver(args);

	}	
}
