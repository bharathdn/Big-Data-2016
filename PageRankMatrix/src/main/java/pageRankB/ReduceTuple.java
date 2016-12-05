package pageRankB;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class ReduceTuple implements Writable {
	
	private LongWritable parentId = new LongWritable();
	private DoubleWritable value = new DoubleWritable();
	private BooleanWritable isRankValue = new BooleanWritable(false);
	
	public ReduceTuple() {
	}
	
	public ReduceTuple(LongWritable parentId) {
		this.parentId = parentId;
	}
	
	public ReduceTuple(LongWritable parentId, DoubleWritable value, BooleanWritable isRankValue) {
		this.parentId = parentId;
		this.value = value;
		this.isRankValue = isRankValue;
	}

	public LongWritable getParentId() {
		return parentId;
	}

	public void setParentId(LongWritable parentId) {
		this.parentId = parentId;
	}

	public DoubleWritable getValue() {
		return value;
	}

	public void setValue(DoubleWritable value) {
		this.value = value;
	}

	public BooleanWritable getIsRankValue() {
		return isRankValue;
	}

	public void setIsRankValue(BooleanWritable isRankValue) {
		this.isRankValue = isRankValue;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		parentId.write(out);
		value.write(out);
		isRankValue.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		parentId.readFields(in);
		value.readFields(in);
		isRankValue.readFields(in);
	}
	
	@Override
	public String toString() {
		return parentId.toString() + "\t" + value + "\t" + isRankValue.get(); 
	}
}