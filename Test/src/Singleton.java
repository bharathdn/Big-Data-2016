public class Singleton {

	//singleton
	private  static Singleton singletonInstance = null; 

	// private ctor
	private Singleton(){
	}

	// using synchronized keyword to avoid race conditions
	public static synchronized Singleton getInstance(){
		if(singletonInstance == null)
			singletonInstance = new Singleton();
		return singletonInstance;
	}
}
