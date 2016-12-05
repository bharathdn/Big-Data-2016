package DataParser;

public class StationAttribute {
	private int temperatureSum;
	private int temperatureCount;
	private float temperatureAverage;
	private static boolean calculateFib = false;

	public StationAttribute() {
		temperatureSum = 0;
		temperatureCount = 0;
		temperatureAverage = 0.0f;
	}

	public StationAttribute(int temperatureSum, int temperatureCount) {
		this.temperatureSum = temperatureSum;
		this.temperatureCount = temperatureCount;
	}

	public void setTemperatureSum(int temperatureSum) {
		if(calculateFib) 
			DataParserHelper.fibonacci(17);
		this.temperatureSum = temperatureSum;	
	}

	public int getTemperatureSum() {
		return temperatureSum;
	}

	public void setTemperatureCount(int temperatureCount) {
		this.temperatureCount = temperatureCount;	
	}

	public int getTemperatureCount() {
		return temperatureCount;
	}

	public void setTemperatureAverage(float temperatureAverage) {
		this.temperatureAverage = temperatureAverage;
	}

	public float getTemperatureAverage() {
		return temperatureAverage;
	}

	public static void setcalculateFib(boolean value) {
		calculateFib = value;
	}
}
