package Utils;

import java.util.ArrayList;
import java.util.List;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public class InstanceHelper {
	
	public static List<String> getStates() {
		List<String> states = new ArrayList<>();
		for (State i : State.values()) {
			states.add(i.toString());
		}
		return states;
	}
	
	
	public static List<String> getCounties() {
		List<String> counties = new ArrayList<>();
		for (County i : County.values()) {
			counties.add(i.toString());
		}
		return counties;
	}
	
	
	public static List<String> getCountries() {
		List<String> countries = new ArrayList<>();
		for (Country i : Country.values()) {
			countries.add(i.toString());
		}
		return countries;
	}
	
	public static ArrayList<Attribute> getAttributes() {
		ArrayList<Attribute> attributes = new ArrayList<>();
		
		attributes.add(new Attribute(Constants.YEAR, Attribute.NUMERIC));
		attributes.add(new Attribute(Constants.MONTH, Attribute.NUMERIC));
		attributes.add(new Attribute(Constants.DAY, Attribute.NUMERIC));
		attributes.add(new Attribute(Constants.TIME, Attribute.NUMERIC));
		attributes.add(new Attribute(Constants.COUNTRY, getCountries()));
		attributes.add(new Attribute(Constants.STATE_PROVINCE, getStates()));
		attributes.add(new Attribute(Constants.COUNTY, getCounties()));
		attributes.add(new Attribute(Constants.EFFORT_DISTANCE_KM, Attribute.NUMERIC));
		attributes.add(new Attribute(Constants.NUMBER_OBSERVERS, Attribute.NUMERIC));
		attributes.add(new Attribute(Constants.Agelaius_phoeniceus, Attribute.NUMERIC));
		
		return attributes;
	}
	
	public static Instance getInstance(Instances instances, Features features) {
		Instance instance = new DenseInstance(Constants.FEATURE_COUNT);
		instance.setDataset(instances);
		
		instance.setValue(0, features.getYear());
		instance.setValue(1, features.getMonth());
		instance.setValue(2, features.getDay());
		instance.setValue(3, features.getTime());
		instance.setValue(4, getCountryValue(features.getCountry()));
		instance.setValue(5, getStateValue(features.getState()));
		instance.setValue(6, getCountyValue(features.getCounty()));
		instance.setValue(7, features.getEffortDistance());
		instance.setValue(8, features.getNumberOfObservers());
		instance.setValue(9, features.getAgelaiusPhoeniceus());
		
		return instance;
	} 
	
	
	public static int getCountryValue(String counrty) {
		Country c = Country.OTHER;
		try {
			c = Country.valueOf(counrty);
		} catch (IllegalArgumentException ex) {
			c = Country.OTHER;
		}
		// -1 because, the indices are stored on 0 based index in attibutes
		return c.getValue() - 1;
	}
	
	
	public static int getStateValue(String state) {
		State s = State.OTHER;
		try {
			s = State.valueOf(state);
		} catch (IllegalArgumentException ex) {
			s = State.OTHER;
		}
		
		// -1 because, the indices are stored on 0 based index in attibutes
		return s.getValue() - 1 ;
	}
	
	
	public static int getCountyValue(String state) {
		County c = County.OTHER;
		try {
			c = County.valueOf(state);
		} catch (IllegalArgumentException ex) {
			c = County.OTHER;
		}
		// -1 because, the indices are stored on 0 based index in attibutes
		return c.getValue() - 1;
	}
	
}