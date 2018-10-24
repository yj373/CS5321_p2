package util;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import data.Tuple;

/**
 * This class is to randomly generate 
 * @author Yixuan Jiang
 *
 */
public class RandomGenerator {
	
	private int upperBound;
	private int lowerBound;
	private int numTuple;
	private String address;
	private Map<String, Integer> schema;
	private int numAttribute;
	
	
	
	
	//Constructors
	public RandomGenerator(int up, int low, int num, String add, Map<String, Integer> sch) {
		this.upperBound = up;
		this.lowerBound = low;
		this.numTuple = num;
		this.address = add;
		this.schema = sch;
		this.numAttribute = sch.keySet().size();
	}
	
//	public randomGenerator(int up, int low, int num, String add, int numAttr ) {
//		this.upperBound = up;
//		this.lowerBound = low;
//		this.numTuple = num;
//		this.address = add;
//		this.numAttribute = numAttr;
//	}
	
	/**
	 * Generate tuples randomly.
	 * Write to corresponding file
	 * @throws IOException 
	 */
	public void generate() throws IOException {
		TupleWriter tw = new TupleWriter(address);
		long[] data = new long[numAttribute];
		for (int i = 0; i < numTuple; i++) {
			for (int j = 0; j < numAttribute; j++) {
				data[j] = ThreadLocalRandom.current().nextInt(lowerBound, upperBound+1);
			}
			Tuple t = new Tuple(data, schema);
			tw.writeTuple(t);
		}
		Tuple terminate = null;
		tw.writeTuple(terminate);
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
	
	//Test random generator
//	public static void main(String[] args) throws Exception {
//		TupleReader test = new TupleReader("Boats AS B");
//		Tuple tuple = test.readNextTuple();
//		String address = "src/randomGenerated/Boats";
//		int up = 5000;
//		int low = 0;
//		int num = 5;
//		RandomGenerator rg = new RandomGenerator(up, low, num, address, tuple.getSchema());
//		rg.generate();

//	}

}
