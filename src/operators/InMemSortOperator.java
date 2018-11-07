package operators;

import java.io.IOException;
import java.util.*;

import data.Dynamic_properties;
import data.Tuple;
import util.TupleReader;
import util.TupleWriter;

/**
 * This class is for the in memory sort before the sort-merge Join Operations.
 * 
 * Although all data are sorted in memory, it still writes all sorted data to 
 * the temp/sorted directory such that the SMJ Operator will reset every state by
 * going back to the file position, and thus avoiding the unbounded state of SMJ
 * operator.
 * 
 * @author Ruoxuan Xu
 *
 */
public class InMemSortOperator extends Operator{
	/* the priority list of columns to be sorted. eg [S.A, S.B, S.C] */
    private List<String> sortColumns;
    private String tempFileAddress;
	private TupleReader tr;
	
    public InMemSortOperator(Operator op1, List<String> sortColumns) {
		this.leftChild = op1;
		this.sortColumns = sortColumns;
		this.schema = op1.schema;
		readSortWrite();
		this.tr = new TupleReader(this.tempFileAddress, this.schema);
		StringBuilder sb = new StringBuilder();
		sb.append("imSort-");
		sb.append(op1.name);
		name = sb.toString();
	}
    
    // Initializing function:
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
    
    //Initializing function:
    /* dump the data to the disk with tupleWriter */
    private void writeSortedBinary(List<Tuple> dataCollection) {
    	StringBuilder output = new StringBuilder(Dynamic_properties.tempPath);
    	if (sortColumns == null) {
    		output.append("/in-memory-sort/no-condition-found");
    	} else {
    		output.append("/in-memory-sort/sortBy" + sortColumns.get(0));
    	}
    	this.tempFileAddress = output.toString();
    	TupleWriter write = new TupleWriter(output.toString());
    	try {
    		for (Tuple tp : dataCollection) {
    			write.writeTuple(tp);
    		}
    		write.writeTuple(null);
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
			this.tr.reset();
		} catch (Exception e) {
			e.printStackTrace();
			e.getMessage();
		}
		
	}
	
	@Override
	public void reset(int idx) {
		this.tr.resetFileChannel(idx);
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
