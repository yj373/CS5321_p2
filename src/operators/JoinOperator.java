package operators;

import java.util.*;

import data.Tuple;
import operators.SortOperator.TupleComparator;


/**
 * JoinOperator class, as a sub class of Operator, is to get next tuple from its children
 * and concatenate them as its own tuple. JoinOperator only joins tuples without considering
 * join conditions. Join conditions will be considered in the following selectOperator.
 * 
 * @author Ruoxuan Xu
 *
 */
public class JoinOperator extends Operator{
    private Tuple currLeftTup;
    private Tuple currRightTup;
    
	/**
	 * Constructor: create an JoinOperator instance with its two child operator.
	 * @param op1 leftChild Operator
	 * @param op2 rightChild Operator
	 */
	public JoinOperator(Operator op1, Operator op2) {
		LinkedList<Operator> childList = new LinkedList<Operator>();
		childList.add(op1);
		childList.add(op2);
		super.setChild(childList);
		setChild(childList);
	    currLeftTup = null;
	    currRightTup = null;
	}

	/**
	 * @return the Tuple joined from the leftChild Operator and rightChild Operator.
	 */
	@Override
	public Tuple getNextTuple() {		
		/* Corner Case: when there are less than two operators under join operator.*/
		LinkedList<Operator> childList = super.getChild();
		if (childList.get(0)== null && childList.get(1) == null) {
			return null;
		}
		if (childList.get(0) == null || childList.get(1) == null) {
			return childList.get(0) == null ? childList.get(1).getNextTuple() : childList.get(0).getNextTuple();
		}

		/* If currLeftTup and currRightTup are both null, it is the start of join
		 *  If currLeftTup is null but currRightTup is not null, it is the end of join 
		 */
	    if (currLeftTup == null) {
	    	if (currRightTup == null) {
		    	currLeftTup = childList.get(0).getNextTuple();
		    	currRightTup = childList.get(1).getNextTuple();
	    	} else {
	    		return null;
	    	}
	    } else {
	    	if (currRightTup == null) {
		    	childList.get(1).reset();
		    	currLeftTup = childList.get(0).getNextTuple();
		    	currRightTup = childList.get(1).getNextTuple();
		    } else {
		    	currRightTup = childList.get(1).getNextTuple();	
		    }	    	
	    }
   
	    if ( currLeftTup != null && currRightTup != null) {
	    	return concatenate(currLeftTup, currRightTup);
	    }
		return this.getNextTuple();
	}
	
	/**
	 * Concatenate tuple1 and tuple2 and return a new Tuple
	 * @return the new concatenated tuple
	 * @param t1 the leading tuple
	 * @param t2 the following tuple
	 */
    private Tuple concatenate(Tuple t1, Tuple t2) { 
    	/* deal with corner case */
    	if (t1 == null && t2 == null) {
    		return null;
    	}
    	if (t1 == null || t2 == null) {
    		return t1 == null ? 
    				     new Tuple(t2.getData(), t2.getSchema()) 
    				     : new Tuple(t1.getData(), t1.getSchema());
    	}
    	
    	/* compose the new data */
    	long[] data = Arrays.copyOf(t1.getData(), t1.getSize() + t2.getSize());
    	System.arraycopy(t2.getData(), 0, data, t1.getSize(), t2.getSize());
    	
    	/* compose the new schema */
    	Map<String, Integer> schema = new HashMap<>();
    	for (Map.Entry<String, Integer> e : t1.getSchema().entrySet()) {
    		schema.put(e.getKey(), e.getValue());
    	}
    	for (Map.Entry<String, Integer> e : t2.getSchema().entrySet()) {
    		schema.put(e.getKey(), e.getValue() + t1.getSize());
    	}
    	
    	/* construct the result tuple */
    	Tuple result = new Tuple(data, schema);
    	return result;
	}

    /**
     * Reset the JoinOperator so that when next time getNextTuple is called, it returns 
     * a tuple at first row.
     */
	@Override
	public void reset() {
		LinkedList<Operator> childList = super.getChild();
		if (childList.get(0) != null) {
			childList.get(0).reset();
		}
		if (childList.get(1) != null) {
			childList.get(1).reset();
		}
		
	}
	
	/**
	 * get LeftChild Operator
	 * @return the leftChild
	 */
	public Operator getLeftChild() {
		return super.getChild().get(0);
	}
	
	/**
	 * get rightChild Operator
	 * @return the rightChild
	 */
	public Operator getRightChild() {
		return super.getChild().get(1);
	}
	
	/**
	 * set the leftChild Operator
	 * @param the leftChild Operator to set as
	 */
	
	public void setLeftChild(Operator op) {
		LinkedList<Operator> newChild = super.getChild();
		newChild.remove(0);
		newChild.addFirst(op);
		super.setChild(newChild);
	}
	
	/**
	 * set the rightChild Operator
	 * @param the rightChild Operator to set as
	 */
    public void setRightChild(Operator op) {
    	LinkedList<Operator> newChild = super.getChild();
    	newChild.remove(1);
		newChild.add(op);
		super.setChild(newChild);
    }
}