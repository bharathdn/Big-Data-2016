package parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import pageRank.Helper;

public class AliasGraphMatrix {

	/*
	 * Mapper for Matrix 
	 */
	public static class NodeMapper 
	extends Mapper<Object, Text, Text, Text> {

		private static Map<String, Long> nodeMap = new HashMap<String, Long>();  

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
					String nodeValue = lineSplit[1];
					nodeMap.put(nodeValue, nodeId);
				}
			}

		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// value => { parentNodeId	adjList }
			// eg: { A	=> 	B,C,D
			String[] textSplit = value.toString().split("\t");
			String nodeIdStr = textSplit[0];
			Text nodeId = new Text(nodeMap.get(nodeIdStr).toString());
			Text nodes = new Text();

			if(textSplit.length > 2) { 
				nodes = getStringAsAdjList(textSplit[2]);
				context.write(nodeId, nodes);
			}			
		}


		public static Text getStringAsAdjList(String nodes) {
			StringBuilder sb = new StringBuilder();
			String[] outLinks = nodes.split("\\|");

			for (int i = 0; i < outLinks.length; i++) {
				if(nodeMap.get(outLinks[i]) != null) {
					sb.append(nodeMap.get(outLinks[i])).append("|");
				}
			}
			if(sb.length() > 0)
				sb.deleteCharAt(sb.lastIndexOf("|") - 1);

			return new Text(sb.toString());
		}
	}


	/*
	 * DRIVER
	 */
	public static void NodeAliasDriver(String[] args) 
			throws Exception { 

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PageRankMatrixAliaserFull");

		job.addCacheFile(new Path(args[0]).toUri());
		
		job.setNumReduceTasks(1);

		job.setJarByClass(AliasGraphMatrix.class);
		job.setMapperClass(NodeMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
	}


	public static void main(String[] args) throws Exception {
//		Helper.deleteDir(new File(args[2]));
		NodeAliasDriver(args);
	}	
}