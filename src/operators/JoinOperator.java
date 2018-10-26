package operators;

import java.util.*;

import data.Tuple;
import net.sf.jsqlparser.expression.Expression;


/**
 * JoinOperator class, as a sub class of Operator, is to get next tuple from its children
 * and concatenate them as its own tuple. JoinOperator only joins tuples without considering
 * join conditions. Join conditions will be considered in the following selectOperator.
 * 
 * @author Ruoxuan Xu
 *
 */
public class JoinOperator extends Operator{
	protected Tuple currLeftTup;
	protected Tuple currRightTup;
	

	
	/**
	 * Constructor: create an JoinOperator instance with its two child operator.
	 * @param op1 leftChild Operator
	 * @param op2 rightChild Operator
	 */
	public JoinOperator(Operator op1, Operator op2, Expression expression) {
		setExpression(expression);
		setLeftChild(op1);
		setRightChild(op2);
		schema = concateSchema(op1.schema, op2.schema);
		currLeftTup = null;
		currRightTup = null;
		StringBuilder sb = new StringBuilder();
		sb.append("join-");
		sb.append(op1.name);
		sb.append("-");
		sb.append(op2.name);
		name = sb.toString();
	}

	/**
	 * @return the Tuple joined from the leftChild Operator and rightChild Operator.
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple t = getNextPureTuple();
		if(t!=null && exp != null) {
			while(!judgeExpression(t)) {
				t = getNextPureTuple();
				if (t==null) break;
			}
		}		
		return t;

	}

	private Tuple getNextPureTuple() {		
		/* Corner Case: when there are less than two operators under join operator.*/
		Operator left = getLeftChild();
		Operator right = getRightChild();
		if (left == null && right == null) {
			return null;
		}
		if (left == null || right == null) {
			return left == null ? right.getNextTuple() : left.getNextTuple();
		}

		/* If currLeftTup and currRightTup are both null, it is the start of join
		 *  If currLeftTup is null but currRightTup is not null, it is the end of join 
		 */
		if (currLeftTup == null) {
			if (currRightTup == null) {
				currLeftTup = left.getNextTuple();
				currRightTup = right.getNextTuple();

				// if right table is empty 
				if(currRightTup == null) {
					if (this.getExpression() == null) {
						return currLeftTup;
					}else {
						return null;
					}

				}
				// if right table is not empty, but left table is empty
				if(currLeftTup == null) {
					if (this.getExpression() == null) {
						return currRightTup;
					}else {
						return null;
					}
				}
			} else {
				// left tuple is null but right tuple is not null, the end 
				// of file or left table if empty
				return null;
			}
		} else {
		// currLeft is not null
			if (currRightTup == null) {
				right.reset();
				currLeftTup = left.getNextTuple();
				currRightTup = right.getNextTuple();
			} else {
				currRightTup = right.getNextTuple();	
			}	    	
		}
		
		// After dealing with all null and not null cases, we exclude 
		// The empty table cases.

		if ( currLeftTup != null && currRightTup != null) {
			Tuple res = concatenate(currLeftTup, currRightTup);
			//return judgeExpression(res) ? res : this.getNextTuple();
			return res;
		}
		return this.getNextPureTuple();
		//return null;
	}

	/**
	 * Concatenate tuple1 and tuple2 and return a new Tuple
	 * @return the new concatenated tuple
	 * @param t1 the leading tuple
	 * @param t2 the following tuple
	 */
	protected Tuple concatenate(Tuple t1, Tuple t2) { 
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
		Map<String, Integer> schema = new HashMap<String, Integer>();
		
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

	public Map<String, Integer> concateSchema(Map<String, Integer> schema1, Map<String, Integer> schema2) {
		
		Map<String, Integer> schema = new HashMap<>();
		for (Map.Entry<String, Integer> e : schema1.entrySet()) {
			schema.put(e.getKey(), e.getValue());
		}
		for (Map.Entry<String, Integer> e : schema2.entrySet()) {
			schema.put(e.getKey(), e.getValue() + schema1.size());
		}

		return schema;
	}	

	/**
	 * Reset the JoinOperator so that when next time getNextTuple is called, it returns 
	 * a tuple at first row.
	 */
	@Override
	public void reset() {
		Operator left = getLeftChild();
		Operator right = getRightChild();
		if (left != null) {
			left.reset();
		}
		if (right != null) {
			right.reset();
		}

	}
}