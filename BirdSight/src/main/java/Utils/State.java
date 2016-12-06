package Utils;

public enum State {
	
	Mississippi(1),Oklahoma(2),Wyoming(3),Minnesota(4),Illinois(5),Georgia(6),Arkansas(7),Ohio(8),Indiana(9),Maryland(10),Louisiana(11),New_Hampshire(12),
	Texas(13),New_York(14),Arizona(15),Iowa(16),Michigan(17),Kansas(18),Utah(19),Virginia(20),Oregon(21),District_of_Columbia(22),Connecticut(23),Montana(24),
	California(25),Idaho(26),New_Mexico(27),South_Dakota(28),Mayaguez(29),Massachusetts(30),Vermont(31),Delaware(32),Pennsylvania(33),Florida(34),Kentucky(35),
	Tennessee(36),Nebraska(37),North_Dakota(38),Missouri(39),Wisconsin(40),Alabama(41),New_Jersey(42),Colorado(43),Washington(44),West_Virginia(45),
	South_Carolina(46),Rhode_Island(47),North_Carolina(48),Nevada(49),Maine(50),OTHER(51);

	
	private int value;
	
	private State(int value) {
		this.value = value;
	}

	public int getValue() {
		return value;
	}
};
