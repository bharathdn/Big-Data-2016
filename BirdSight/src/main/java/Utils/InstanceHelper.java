package Utils;



import java.util.ArrayList;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public class InstanceHelper {
	
	static ArrayList<String> features;
	
//	public InstanceHelper() {
//		features = new ArrayList<>();
//		
//		features.add(Features.YEAR);
//		features.add(Features.MONTH);
//		features.add(Features.DAY);
//		features.add(Features.TIME);
//		features.add(Features.COUNTRY);
//		features.add(Features.STATE_PROVINCE);
//		features.add(Features.COUNTY);
//		features.add(Features.EFFORT_DISTANCE_KM);
//		features.add(Features.NUMBER_OBSERVERS);
//		features.add(Features.Agelaius_phoeniceus);	
//	}
	

//	public static Instance getInstance(Instances instances,String text) {
//		Instance instance = new DenseInstance(Constants.FEATURE_COUNT);
//		instance.setDataset(instances);
//		
//		System.out.println(text);
//		String[] textSplit = text.split(",");
//		System.out.println("\n\nTextSplit length :: " + textSplit.length);
//		
//		instance.setValue(Features.YEAR_INDEX, textSplit[Features.YEAR_INDEX]);
//		instance.setValue(Features.MONTH_INDEX, textSplit[Features.YEAR_INDEX]);
//		instance.setValue(Features.DAY_INDEX, textSplit[Features.DAY_INDEX]);
//		instance.setValue(Features.TIME_INDEX, textSplit[Features.TIME_INDEX]);
//		instance.setValue(Features.COUNTRY_INDEX, textSplit[Features.COUNTRY_INDEX]);
//		instance.setValue(Features.STATE_PROVINCE_INDEX, textSplit[Features.STATE_PROVINCE_INDEX]);
//		instance.setValue(Features.COUNTY_INDEX, textSplit[Features.COUNTY_INDEX]);
//		instance.setValue(Features.NUMBER_OBSERVERS_INDEX, textSplit[Features.NUMBER_OBSERVERS_INDEX]);
//		instance.setValue(Features.Agelaius_phoeniceus_INDEX, textSplit[Features.Agelaius_phoeniceus_INDEX]);
//		
//		return instance;
//	} 
	
	
	public static ArrayList<Attribute> getAttributes() {
		ArrayList<Attribute> attributes = new ArrayList<>();
		
		attributes.add(new Attribute(Constants.YEAR, Attribute.NUMERIC));
		attributes.add(new Attribute(Constants.MONTH, Attribute.NUMERIC));
		attributes.add(new Attribute(Constants.DAY, Attribute.NUMERIC));
		attributes.add(new Attribute(Constants.TIME, Attribute.NUMERIC));
		attributes.add(new Attribute(Constants.COUNTRY, Attribute.STRING));
		attributes.add(new Attribute(Constants.STATE_PROVINCE, Attribute.STRING));
		attributes.add(new Attribute(Constants.COUNTY, Attribute.STRING));
		attributes.add(new Attribute(Constants.EFFORT_DISTANCE_KM, Attribute.NUMERIC));
		attributes.add(new Attribute(Constants.NUMBER_OBSERVERS, Attribute.NUMERIC));
		attributes.add(new Attribute(Constants.Agelaius_phoeniceus, Attribute.NUMERIC));
		
		return attributes;
	}
	
}