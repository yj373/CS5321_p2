package util;

import java.util.Map;

/**
 * This class is to randomly generate 
 * @author Yixuan Jiang
 *
 */
public class randomGenerator {
	
	private int upperBound;
	private int lowerBound;
	private int numTuple;
	private String address;
	private Map<String, Integer> schema;
	private int numAttribute;
	
	
	//Constructors
	public randomGenerator(int up, int low, int num, String add, Map<String, Integer> sch) {
		this.upperBound = up;
		this.lowerBound = low;
		this.numTuple = num;
		this.address = add;
		this.schema = sch;
		this.numAttribute = sch.keySet().size();
	}
	
	public randomGenerator(int up, int low, int num, String add, int numAttr ) {
		this.upperBound = up;
		this.lowerBound = low;
		this.numTuple = num;
		this.address = add;
		this.numAttribute = numAttr;
	}
	
	/**
	 * Generate tuples randomly
	 * Write to corresponding file
	 */
	public void generate() {
		
	}
	
	//Getters and Setters
	public int getUpperBound() {
		return upperBound;
	}

	public void setUpperBound(int upperBound) {
		this.upperBound = upperBound;
	}

	public int getLowerBound() {
		return lowerBound;
	}

	public void setLowerBound(int lowerBound) {
		this.lowerBound = lowerBound;
	}

	public int getNumTuple() {
		return numTuple;
	}

	public void setNumTuple(int numTuple) {
		this.numTuple = numTuple;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public Map<String, Integer> getSchema() {
		return schema;
	}

	public void setSchema(Map<String, Integer> schema) {
		this.schema = schema;
	}

	public int getNumAttribute() {
		return numAttribute;
	}

	public void setNumAttribute(int numAttribute) {
		this.numAttribute = numAttribute;
	}
	
	

}
