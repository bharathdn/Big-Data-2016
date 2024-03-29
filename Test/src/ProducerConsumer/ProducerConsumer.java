package ProducerConsumer;

public class ProducerConsumer {

	private static Object lock = new Object();
	private static int[] buffer;
	private static int count;

	static class Producer {
		void produce() {
			synchronized (lock) {
				System.out.println("In side producer:: count - "+ count);
				if (isFull()) {
					try {
						lock.wait();
					} 
					catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				buffer[count++] = 1;
				lock.notify();
			}
		}
	}

	static class Consumer {
		void consume() {
			synchronized (lock) {
				System.out.println("In side consumer:: count - "+ count);
				if (isEmpty()) {
					try {
						lock.wait();	
					} 
					catch (InterruptedException e) {
						e.printStackTrace();
					}					
				}	
				buffer[--count] = 0;
				lock.notify();
			}
		}
	}

	static boolean isEmpty() {
		return count == 0;
	}

	static boolean isFull() {
		return count == buffer.length;
	}

	public static void main(String[] args) throws InterruptedException {
		buffer = new int[10];
		count = 0;

		Producer producer = new Producer();
		Consumer consumer = new Consumer();

		Runnable produceTask = () -> {
			for (int i = 0; i <  50; i++) {
				producer.produce();
			}
			System.out.println("Done Producing");
		};

		Runnable consumeTask = () -> {
			for (int i = 0; i < 39; i++) {
				consumer.consume();
			}
			System.out.println("Done Consuming");
		};

		Thread consumerThread = new Thread(consumeTask);
		Thread producerThread = new Thread(produceTask);

		consumerThread.start();
		producerThread.start();

		consumerThread.join();
		producerThread.join();

		System.out.println("Data left in the buffer : " + count);
	}
}
