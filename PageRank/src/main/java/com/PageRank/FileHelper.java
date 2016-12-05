package com.PageRank;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

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
}
