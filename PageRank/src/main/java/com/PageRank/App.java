package com.PageRank;

public class App 
{
    public static void main(String[] args) throws Exception
    {
		if(args.length < 3){
			System.out.println("Input ERROR: Arguments Insufficient");
			return;
		}
	
		PageRankMgr.performPageRank(args);
    }
}
