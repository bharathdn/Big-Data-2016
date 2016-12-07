package Utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import weka.classifiers.Classifier;
import weka.classifiers.trees.RandomTree;
import weka.core.SerializationHelper;

public class FileHelper {

	public static void write(String stringToWrite, String filePath)
	{
		try (BufferedWriter inLinkFile = new BufferedWriter(new FileWriter(filePath,true)))
		{
			inLinkFile.write(stringToWrite);
		}
		catch (Exception e)
		{
			System.out.println("Error while writing Link Url file");
			e.printStackTrace();
		}
	}


	// used to delete directory while debugging on IDE 
	public static boolean deleteDir(File dir) 
	{ 
		if (dir.isDirectory()) 
		{ 
			String[] children = dir.list(); 
			for (int i=0; i<children.length; i++)
			{ 
				boolean success = deleteDir(new File(dir, children[i])); 
				if (!success) 
					return false; 
			} 
		}  
		// The directory is now empty and can be deleted
		// or it is a file. so delete it
		return dir.delete(); 
	}
	
	public static boolean deleteFile(File file) 
	{ 
		if(file.exists())
			return file.delete();
		return file.delete();
	}
	
	public static void writeClassifierToFile(RandomTree classfier, Reducer<Text, Text, Text, Text>.Context context, String key) 
			throws Exception {
		Configuration conf = context.getConfiguration();
		String modelPath = "model/";
		FileSystem fs = FileSystem.get(URI.create(modelPath), conf);
		FSDataOutputStream outputStream = fs.create(new Path(modelPath + key));
		
		SerializationHelper.write(outputStream, classfier);
	}
	
	public static Classifier readClassifierFromFile(Reducer<Text, Text, Text, NullWritable>.Context context, String key) 
			throws Exception {
		Configuration conf = context.getConfiguration();
		String modelPath = "model/";
		FileSystem fs = FileSystem.get(URI.create(modelPath), conf);
		InputStream in = fs.open(new Path(modelPath + key));
				
		ObjectInputStream objectStream = new ObjectInputStream(in);
		return (Classifier) objectStream.readObject();
	} 
}
