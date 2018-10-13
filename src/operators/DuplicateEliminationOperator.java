package operators;

import java.util.LinkedList;
import data.Tuple;
import net.sf.jsqlparser.statement.select.PlainSelect;


/**
 * This class provides function:
 * Eliminate duplication in output tuples
 * 
 * @author Xiaoxing Yan
 *
 */

public class DuplicateEliminationOperator extends Operator{

	private Tuple prevTuple;
	/*check DISTINCT keyword*/
	boolean workState; 

	/** 
	 * This method is a constructor which is to
	 * check the DISTINCT key word in the query and then
	 * initialize related fields
	 * 
	 * @param plainSelect  PlainSelect of query
	 * @param op  pass in child operator
	 * 
	 */
	

	public DuplicateEliminationOperator(PlainSelect ps, Operator op) {
		if (ps.getDistinct()!=null) {
			workState = true;
		}else {
			workState = false;
		}
		LinkedList<Operator> newChild = new LinkedList<Operator>();
		newChild.add(op);
		super.setChild(newChild);
	}
	
	
	/**
	 * This method is to get the next tuple after eliminating duplication
	 * 
	 * @return next tuple 
	 */

	@Override
	public Tuple getNextTuple() {
		Operator child = super.getChild().get(0);
		Tuple t = child.getNextTuple();

		if(workState) {
			if (prevTuple != null) {
				while (t!= null && isEqual(t, prevTuple)) {
					t = child.getNextTuple();
				}
			}
		}
		prevTuple = t;

		return t;
	}
	
	/**
	 * This method is to reset operator
	 * by resetting its child operator
	 */

	@Override
	public void reset() {
		super.getChild().get(0).reset();

	}


	/**
	 * get previous Tuple
	 * 
	 * @return Tuple
	 */
	public Tuple getPrevTuple() {
		return prevTuple;
	}
	
	/**
	 * set previous Tuple
	 * 
	 * @return Tuple
	 */

	public void setPrevTuple(Tuple prevTuple) {
		this.prevTuple = prevTuple;
	}

	/**
	 * check if two Tuples are the same
	 *
	 * @param t1 Tuple 1
	 * @param t2 Tuple 2
	 * @return check result
	 */
	public boolean isEqual(Tuple t1, Tuple t2) {
		if (t1.getSize()!=t2.getSize()) return false;
		for (int i=0; i<t1.getSize(); i++) {
			if (t1.getData()[i] != t2.getData()[i]) return false;
		}
		return true;

	}



}