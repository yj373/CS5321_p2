package test;

import data.Tuple;
import util.RandomGenerator;
import util.TupleReader;

public class experiments {
	
	public void main (String[] args) throws Exception {
		
		TupleReader test = new TupleReader("Boats AS B");
		Tuple tuple = test.readNextTuple();
		String address = "src/randomGenerated/Boats";
		int up = 10000;
		int low = 0;
		int num = 6000;
		RandomGenerator rg = new RandomGenerator(up, low, num, address, tuple.getSchema());
		rg.generate();


	}

}
