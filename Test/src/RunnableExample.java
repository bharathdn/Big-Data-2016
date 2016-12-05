
public class RunnableExample implements Runnable {
	
	private String data;
	
	public RunnableExample(String str) {
		data = str;
	}
	
	private void PrintString() {
		System.out.println(data);
	}
	
	public void run() {
		PrintString();
	}
}
