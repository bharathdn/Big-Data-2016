package Utils;



import java.util.ArrayList;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;

public class InstanceHelper {

	public static Instance getInstance(String text) {
		Instance instance = new DenseInstance(10);
		
		String[] textSplit = text.split(",");
		for (int i = 0; i < textSplit.length; i++) {
			instance.setValue(i, textSplit[i]);
		}
		return instance;
	} 
	
	public static ArrayList<Attribute> getAttributes() {
		ArrayList<Attribute> attributes = new ArrayList<>();
		
		attributes.add(new Attribute("year"));
		attributes.add(new Attribute("month"));
		attributes.add(new Attribute("day"));
		attributes.add(new Attribute("time"));
		attributes.add(new Attribute("country"));
		attributes.add(new Attribute("state"));
		attributes.add(new Attribute("county"));
		attributes.add(new Attribute("effortDist"));
		attributes.add(new Attribute("numberOfObservers"));
		attributes.add(new Attribute("agelaiusPhoeniceus"));
		
		return attributes;
	}
	
}