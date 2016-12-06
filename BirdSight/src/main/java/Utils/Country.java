package Utils;

public enum Country {
	
	United_States(1), OTHER(2);
	
	private int value;
	
	private Country(int value) {
		this.value = value;
	}

	public int getValue() {
		return value;
	}
	
};