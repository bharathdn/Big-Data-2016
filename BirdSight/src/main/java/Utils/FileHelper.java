package Utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import weka.classifiers.bayes.NaiveBayes;
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
	
	public static void writeModelToFile(RandomTree classfier ,Reducer<Text, Text, Text, Text>.Context context, String key) 
			throws Exception {
		Configuration conf = context.getConfiguration();
		//String modelPath = conf.get(Constants.MODEL);
		String modelPath = "model/";
//		System.out.println("MODEL PATH ->" + modelPath);
		FileSystem fs = FileSystem.get(URI.create(modelPath), conf);
		FSDataOutputStream outputStream = fs.create(new Path(modelPath + key));
		
		SerializationHelper.write(outputStream, classfier);
	}
}
