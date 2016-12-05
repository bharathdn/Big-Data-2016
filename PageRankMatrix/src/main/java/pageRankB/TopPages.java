package pageRankB;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TopPages {
	
	/*
	 * Mapper for Matrix 
	 */
	public static class TopPagesMapper 
	extends Mapper<Object, Text, NullWritable, Text> {

		private TreeMap<Double, Long> topNodeMap = new TreeMap<Double, Long>(Collections.reverseOrder());

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] textSplit = value.toString().split("\t");
			
			Long nodeId = new Long(textSplit[0]);
			Double pageRank = new Double(textSplit[1]);
			
			topNodeMap.put(pageRank, nodeId);
			
			if(topNodeMap.size() > 100) {
				// if size > 100, then remove the entry with least pageRank value
				topNodeMap.remove(topNodeMap.lastKey());
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for(Map.Entry<Double, Long> topEntrySet : topNodeMap.entrySet()) {
				context.write(NullWritable.get(), new Text(topEntrySet.getKey() +"|"+topEntrySet.getValue()));
			}
		}
	}
	
	/*
	 * REDUCE CLASS
	 */
	public static class TopPagesReducer 
	extends Reducer<NullWritable, Text, Text, DoubleWritable> {

		private TreeMap<Double, Long> topNodesMap = new TreeMap<Double, Long>();

		// Map that maps 1,2,3 based Nodes to Actual pagenames
		private static Map<Long, String> nodeIdNameMap = new HashMap<Long, String>();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// read the nodeId to String mapping from file
			Configuration conf = context.getConfiguration();
			URI[] uris = context.getCacheFiles();
			
			Path awsPath = new Path(uris[0]);
			FileSystem fs = FileSystem.get(awsPath.toUri(), conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(awsPath)));
			String line;
			while ((line = br.readLine()) != null) {
				if(line.length() > 0) {
					String[] lineSplit = line.split("\t");
					Long nodeId =  Long.parseLong(lineSplit[0]);
					String nodeName = lineSplit[1];
					nodeIdNameMap.put(nodeId, nodeName);
				}
			}
		}
		

		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			for(Text value : values) {
				String[] str = value.toString().split("\\|");
				topNodesMap.put(Double.parseDouble(str[0]), Long.parseLong(str[1]));
				
				if(topNodesMap.size() > 100) {
					topNodesMap.remove(topNodesMap.firstKey());
				}
			}
		}
		
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for(Map.Entry<Double, Long> topEntrySet : topNodesMap.entrySet()) {
				String pageName = nodeIdNameMap.get(topEntrySet.getValue());
				context.write(new Text(pageName), new DoubleWritable(topEntrySet.getKey()));
			}
		}	
	}
	
	
	/*
	 * DRIVER
	 */
	public static void NodeAliasDriver(String[] args) 
			throws Exception { 

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PageRankMatrixAliaser");
		
		job.addCacheFile(new Path(args[6]).toUri());
		
		job.setJarByClass(TopPages.class);
		job.setMapperClass(TopPagesMapper.class);
		job.setReducerClass(TopPagesReducer.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[3]));
		FileOutputFormat.setOutputPath(job, new Path(args[7]));

		job.waitForCompletion(true);
	}
	
	
	public static void main(String[] args) throws Exception {
//		Helper.deleteDir(new File(args[7]));
		System.out.println(args[3]);
		System.out.println(args[7]);
		NodeAliasDriver(args);
	}	

}
