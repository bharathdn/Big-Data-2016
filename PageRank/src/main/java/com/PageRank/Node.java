package com.PageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
/*
 * Class representing a Node in a graph. 
 * A Node consists of:-
 * 		nodeId: Name f the node in Graph
 * 		adjList: List of nodeIds that Node points to in a graph
 */
public class Node implements Writable {
	Text id = new Text();
	DoubleWritable pageRank = new DoubleWritable();
	List<Text> adjList = new ArrayList<>();
	
	// this zero-argument constructor is necessary to use the class in Hadoop
	public Node() {

	}

	public Node(Text nodeId,DoubleWritable pageRank) {
		this.id = nodeId;
		this.pageRank = pageRank;
	}
	
	public Node(DoubleWritable pageRank) {
		this.pageRank = pageRank;
	}

	public Node(ArrayList<Text> adjList, DoubleWritable pageRank) {
		this.adjList = adjList;
		this.pageRank = pageRank;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(adjList.size());
		for(Text nodeId : adjList) {
			nodeId.write(out);
		}

		pageRank.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		adjList = new ArrayList<Text>(size);
		for (int i = 0; i < size; i++) {
			Text nodeId = new Text();
			nodeId.readFields(in);
			adjList.add(nodeId);
		}
		pageRank.readFields(in);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(pageRank.get()).append(getListAsString(adjList));
		return sb.toString();
	}

	/*
	 * Method that takes list of 
	 */
	public static String getListAsString(List<Text> list) {
		StringBuilder sb = new StringBuilder();
		if(list.size() > 0) {
			sb.append("\t");
		}

		for (int i = 0; i < list.size(); i++) {
			sb.append(list.get(i).toString()).append("|");
		}

		if(sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);
		}
		return sb.toString();
	}
}
