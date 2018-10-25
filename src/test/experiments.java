package test;

import data.Tuple;
import util.RandomGenerator;
import util.TupleReader;

/**
 * This class provides function:
 * 
 * generate experiments files
 * 
 * 
 * @author Xiaoxing Yan
 */

public class experiments {
	
	public static void main(String[] args) throws Exception {
		
		TupleReader test = new TupleReader("Reserves AS R");
		Tuple tuple = test.readNextTuple();
		String address = "src/randomGenerated/Reserves";
		int up = 10000;
		int low = 0;
		int num = 6000;
		RandomGenerator rg = new RandomGenerator(up, low, num, address, tuple.getSchema());
		rg.generate();


	}

}
