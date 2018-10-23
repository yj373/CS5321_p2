package operators;

import java.io.IOException;
import java.util.*;

import data.Dynamic_properties;
import data.Tuple;
import util.TupleReader;
import util.TupleWriter;

public class InMemSortOperator extends Operator{
    private List<String> sortColumns;
    private String tempFileAddress;
	private TupleReader tr;
	
    public InMemSortOperator(Operator op1, List<String> sortColumns) {
		this.leftChild = op1;
		this.sortColumns = sortColumns;
		this.schema = op1.schema;
		readSortWrite();
		this.tr = new TupleReader(this.tempFileAddress, this.schema);
	}
    
    // Initializing func:
    // sorted the array with comparator
    private void readSortWrite() {
    	List<Tuple> dataCollection = new ArrayList<>();
    	Tuple curr;
    	// read all data into memory
    	while((curr = leftChild.getNextTuple()) != null) {
    		dataCollection.add(curr);
    	}
    	leftChild.reset();
    	// sort all data
    	Collections.sort(dataCollection, new TupleComparator());
    	
    	// write all data into corresponding directory with specified name
    	writeSortedBinary(dataCollection);
    }
    
    //Initializing func:
    /* dump the data to the disk with tupleWriter */
    private void writeSortedBinary(List<Tuple> dataCollection) {
    	StringBuilder output = new StringBuilder(Dynamic_properties.outputPath);
    	output.append("/in-memory-sort/sortBy" + sortColumns.get(0));
    	this.tempFileAddress = output.toString();
    	TupleWriter write = new TupleWriter(output.toString());
    	try {
    		for (Tuple tp : dataCollection) {
    			write.writeTuple(tp);
    		}
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    }
    
    // like scan Operator, use tupleReader to getNextTuple from a disk file
	@Override
	public Tuple getNextTuple() {
		try {
			return tr.readNextTuple();
		} catch (Exception e) {
			e.printStackTrace();
			e.getMessage();
		}	
		return null;
	}

	@Override
	public void reset() {
		try {
			this.tr.resetBuffer();
			this.tr.resetFileChannel();
		} catch (Exception e) {
			e.printStackTrace();
			e.getMessage();
		}
		
	}
	
	private class TupleComparator implements Comparator<Tuple> {
		@Override
		public int compare(Tuple o1, Tuple o2) {
			if (sortColumns != null) {
				for (int i = 0; i < sortColumns.size(); i++) {
					Integer col = o1.getSchema().get(sortColumns.get(i));
					if (o1.getData()[col] < o2.getData()[col]) {
						return -1;
					} 
					if (o1.getData()[col] > o2.getData()[col]){
						return 1;
					} 
				}
				for (int i = 0; i < schema.size(); i++) {
					if (o1.getData()[i] < o2.getData()[i]) {
						return -1;
					} 
					if (o1.getData()[i] > o2.getData()[i]){
						return 1;
					} 
				}
				return 0;
			}
			/* if sortColumns is null, say there is no join condition in query,
			 * return tuple directly without sorting.
			 */
			return 0;
		}
	}

}
