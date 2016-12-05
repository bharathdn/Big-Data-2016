package pageRankB;

import parserB.Alias;
import parserB.AliasGraphDangling;
import parserB.AliasGraphMatrix;
import parserB.InitializePageRank;

public class App {
	
	private static Long linkCount = 0l;
	private static String initialDir = "";
	
	// constants
	private static final String LINKS = "links";
	private static final String ALIAS = "alias";
	private static final String ALIASMATRIX = "aliasMatrix";
	private static final String ALIASDANGLING = "aliasDangling";
	private static final String PART0 = "/part-r-00000";
	
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Argument count Invalid");
			return;
		}
		
		initialDir = args[0];
		performGraphAliasing(args);
		performPageRank(args);
	}
	
	
	public static void performGraphAliasing(String[] args) throws Exception {
		String[] args1 = new String[2];
		args1[0] = initialDir + LINKS;
		args1[1] = initialDir + ALIAS;
		linkCount = Alias.main(args1);
		
		String[] args2 = new String[3];
		args2[0] = initialDir + ALIAS + "/part-r-00000";
		args2[1] = initialDir + LINKS;
		args2[2] = initialDir + ALIASMATRIX;
		AliasGraphMatrix.main(args2);
		
		String[] args3 = new String[3];
		args3[0] = initialDir + ALIAS + "/part-r-00000";
		args3[1] = initialDir + LINKS;
		args3[2] = initialDir + ALIASDANGLING;
		AliasGraphDangling.main(args3);
		
		String[] args4 = new String[2];
		args4[0] = initialDir + ALIAS + "/part-r-00000";
		args4[1] = initialDir + "op0a";
		InitializePageRank.main(args4, linkCount);
		System.out.println("LINKCOUNT == "+ linkCount);
	}
	
	public static void performPageRank(String[] args) throws Exception {
		
		
		String[] argsInit = new String[8];
		// matrix path does not change
		argsInit[0] = initialDir + ALIASMATRIX;
		// Dangling path, does not change
		argsInit[1] = initialDir + ALIASDANGLING;
		
		// INPUT RANKFILE: path for RankFile that always gets changed by M*R task
		String InputRankPath = initialDir + "op";
		// OUTPUT RANKFILE: path for RankFile that always gets changed by M*R task
		String OutputRankPath = initialDir + "op";
		
		// path for Dangling-op
		String DanglingOutputPath = initialDir + "dangling/op";
		
		// linkcount
		argsInit[5] = linkCount.toString();
		int iterations = 10;
		for (int i = 0; i < iterations; i++) {
			argsInit[2] = InputRankPath + Integer.toString(i) + "a" + "/" + PART0;
			argsInit[3] = OutputRankPath + Integer.toString(i+1);
			argsInit[4] = DanglingOutputPath + Integer.toString(i);
			    
			double dprShare = DanglingNodeMgr.main(argsInit);
			PageRankMgr.main(argsInit, dprShare);
			
			String[] args2 = new String[2];
			args2[0] = argsInit[3];
			args2[1] = InputRankPath + Integer.toString(i + 1) + "a";
			PageRankAggregator.main(args2);
		}
		
		//alias file
		argsInit[6] = initialDir + ALIAS + PART0;
		argsInit[7] = OutputRankPath + Integer.toString(iterations+1);
		argsInit[3] = OutputRankPath + Integer.toString(iterations) + "a";
		TopPages.main(argsInit);
	}	
}
