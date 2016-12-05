
public class Runner {

	public static void main(String[] args) {
		RunnableExample r1 = new RunnableExample("This is Thread1");
		RunnableExample r2 = new RunnableExample("This is Thread2");
		
		Thread t1 = new Thread(r1);
		Thread t2 = new Thread(r2);
		System.out.println("Starting threads");
		t1.start();
		t2.start();
		System.out.println("Running Complete");
	}	
}
