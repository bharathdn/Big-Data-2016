package pageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IndexTuple implements Writable, WritableComparable<IndexTuple>{
	
	private IntWritable indexI;
	private IntWritable indexJ;
	
	public IndexTuple() {
	}
	
	public IndexTuple(IntWritable index1, IntWritable index2) {
		this.indexI = index1;
		this.indexJ = index2;
	}

	public IntWritable getIndex1() {
		return indexI;
	}

	public void setIndex1(IntWritable index1) {
		this.indexI = index1;
	}

	public IntWritable getIndex2() {
		return indexJ;
	}

	public void setIndex2(IntWritable index2) {
		this.indexJ = index2;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		indexI.write(out);
		indexJ.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		indexI.readFields(in);
		indexJ.readFields(in);
	}
	
	@Override
	public String toString() {
		return indexI + "\t" + indexJ; 
	}
	
	@Override
	public int compareTo(IndexTuple ss) {
		return this.indexI.compareTo(ss.indexJ);
	}

}
