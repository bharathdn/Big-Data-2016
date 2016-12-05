package com.WeatherData.com.WeatherData;

public class StationAttribute {
	private int tMaxSum;
	private int tMaxCount;
	private float tMaxAverage;
	
	private int tMinSum;
	private int tMinCount;
	private float tMinAverage;
	
	private static boolean calculateFib = false;

	public StationAttribute() {
		tMaxSum = 0;
		tMaxCount = 0;
		tMaxAverage = 0.0f;
		tMinSum = 0;
		tMinCount = 0;
		tMinAverage = 0.0f;
	}

	
	public int gettMinSum() {
		return tMinSum;
	}


	public void settMinSum(int tMinSum) {
		this.tMinSum = tMinSum;
	}


	public int gettMinCount() {
		return tMinCount;
	}


	public void settMinCount(int tMinCount) {
		this.tMinCount = tMinCount;
	}


	public float gettMinAverage() {
		return tMinAverage;
	}


	public void settMinAverage(float tMinAverage) {
		this.tMinAverage = tMinAverage;
	}


	public StationAttribute(int temperatureSum, int temperatureCount) {
		this.tMaxSum = temperatureSum;
		this.tMaxCount = temperatureCount;
	}

	public void settMaxSum(int temperatureSum) {
		if(calculateFib) 
			DataParserHelper.fibonacci(17);
		this.tMaxSum = temperatureSum;	
	}

	public int gettMaxSum() {
		return tMaxSum;
	}

	public void setTMaxCount(int temperatureCount) {
		this.tMaxCount = temperatureCount;	
	}

	public int getTMaxCount() {
		return tMaxCount;
	}

	public void setTMaxAverage(float temperatureAverage) {
		this.tMaxAverage = temperatureAverage;
	}

	public float getTMaxAverage() {
		return tMaxAverage;
	}

	public static void setcalculateFib(boolean value) {
		calculateFib = value;
	}
	
	public static boolean getcalculateFib() {
		return calculateFib;
	}
}
