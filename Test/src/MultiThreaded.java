
public class MultiThreaded {

	public static void main(String[] args){
		
		Runnable runnable = () -> {
			System.out.println("Running on thread :: " + Thread.currentThread().getName());
		};
		
		Thread t = new Thread(runnable);
		t.setName("My First Thread");
		t.start();
	}
	
}
 