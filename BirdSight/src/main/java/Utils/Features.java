package Utils;

public class Features {
	
	private int year;
	private int month;
	private int day;
	private float time;
	private String country;
	private String state;
	private String county;
	private float effortDistance;
	private int numberOfObservers;
	private int agelaiusPhoeniceus;
	
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public int getMonth() {
		return month;
	}
	public void setMonth(int month) {
		this.month = month;
	}
	public int getDay() {
		return day;
	}
	public void setDay(int day) {
		this.day = day;
	}
	public float getTime() {
		return time;
	}
	public void setTime(float time) {
		this.time = time;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getCounty() {
		return county;
	}
	public void setCounty(String county) {
		this.county = county;
	}
	public float getEffortDistance() {
		return effortDistance;
	}
	public void setEffortDistance(float effortDistance) {
		this.effortDistance = effortDistance;
	}
	public int getNumberOfObservers() {
		return numberOfObservers;
	}
	public void setNumberOfObservers(int numberOfObservers) {
		this.numberOfObservers = numberOfObservers;
	}
	public int getAgelaiusPhoeniceus() {
		return agelaiusPhoeniceus;
	}
	public void setAgelaiusPhoeniceus(int agelaiusPhoeniceus) {
		this.agelaiusPhoeniceus = agelaiusPhoeniceus;
	}
	
	public static Features getFeatures(String text) {
		Features features = new Features();
		String[] textSplit = text.split(",");
		
		features.year = getIntegerValue(textSplit[Constants.YEAR_INDEX].trim());
		features.month = getIntegerValue(textSplit[Constants.MONTH_INDEX].trim());
		features.day = getIntegerValue(textSplit[Constants.DAY_INDEX].trim());
		features.time = getFloatValue(textSplit[Constants.TIME_INDEX].trim());
		features.country = getStringValue(textSplit[Constants.COUNTRY_INDEX].trim());
		features.state = getStringValue(textSplit[Constants.STATE_PROVINCE_INDEX].trim());
		features.county = getStringValue(textSplit[Constants.COUNTY_INDEX].trim());
		features.numberOfObservers = getIntegerValue(textSplit[Constants.NUMBER_OBSERVERS_INDEX].trim());
		features.agelaiusPhoeniceus = getAgelaiusPhoeniceusValue(textSplit[Constants.Agelaius_phoeniceus_INDEX].trim()); 
		
		return features;
	}
	
	private static int getAgelaiusPhoeniceusValue(String value) {
		value  = value.trim();
		int agelaiusPhoeniceus = 0;
		if(value.equals("?") || value.equals("X"))  
			agelaiusPhoeniceus = 0;
		try {
			if(Integer.parseInt(value) > 0)
				agelaiusPhoeniceus = 1;
			if(Integer.parseInt(value) == 0)
				agelaiusPhoeniceus = 0;
		} catch (Exception e) {
			agelaiusPhoeniceus = 0;
		}
		
		return agelaiusPhoeniceus;
	}
	
	// Get Integer Value
	private static int getIntegerValue(String value) {
		int feature = 0;
		if(value.equals("?") || value.equals("X"))  
			feature = 0;
		else
			feature = Integer.parseInt(value);
		
		return feature;
	}
	
	// Get Float Value
	private static float getFloatValue(String value){
		float feature = 0.0f;
		if(value.equals("?") || value.equals("X"))  
			feature = 0.0f;
		else
			feature = Float.parseFloat(value);
		
		return feature;
	}
	
	// Get String value
	private static String getStringValue(String value){
		String feature = "0";
		if(value.equals("?") || value.equals("X"))  
			feature = "";
		else
			feature = value;
		
		return feature;
	}

}