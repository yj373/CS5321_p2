package operators;

import java.io.IOException;
import java.util.Map;

import data.Dynamic_properties;
import data.Tuple;
import net.sf.jsqlparser.expression.Expression;
import util.TupleWriter;
import visitors.BasicExpressionVisitor;
/**
 * parent class of every operator
 * 
 * @author Yixuan Jiang
 *
 */
public abstract class Operator {
	protected Operator leftChild;
	protected Operator rightChild;
	protected Expression exp;
	protected String name;
	
	protected Map<String, Integer> schema;
	/*
	// Marker to indicate if we checked the table beneath the operator is empty or not
	// checkIfEmpty[0] indicates if we checked before;
	// checkIfEmpty[1] indicates if it is empty
	 */
	 
	private boolean[] checkIfEmpty =  new boolean[2];;

	/**
	 * get child operator
	 */
	public Operator getLeftChild() {
		return leftChild;
	}
	public Operator getRightChild() {
		return rightChild;
	}
	

	/**
	 * set child operator
	 */
	public void setLeftChild(Operator child) {
		this.leftChild = child;
	}
	
	public void setRightChild(Operator child) {
		this.rightChild = child;
	}
	
	/**
	 * get the expression
	 * set the expression
	 */
	public Expression getExpression() {
		return exp;
	}
	public void setExpression(Expression expression) {
		this.exp = expression;
	}
	
	/**
	 * get schema
	 * set schema
	 */
	public Map<String, Integer> getSchema(){
		return this.schema;
	}
	public void setSchema(Map<String, Integer> m) {
		this.schema = m;
	}
	/**
	 * get operator name
	 * set operator name
	 */
	public String getOpname() {
		return this.name;
	}
	public void setName(String n) {
		this.name = n;
	}

	/**
	 * Return the next next tuple, if there are some available 
	 * output, otherwise, return null
	 */
	public abstract Tuple getNextTuple();

	/**
	 * Reset the state of the operator, so that
	 * it will output from the beginning
	 */
	public abstract void reset();

	/**
	 * Print out all the output tuple
	 */
	public void dump() {
		//reset();
		Tuple tuple = getNextTuple();
		while (tuple != null) {
			//tuple.printData();
			tuple = getNextTuple();
		}
		reset();
		System.out.println("finishing dumping");
	}
	
    /*should override by External Sort Operator and In-Memory Sort Operator */
	/* idx is the nth entry that we want to trace back and restart with in relation*/
	public void reset(int idx) {
	}
	
	public boolean judgeExpression(Tuple tuple) {
		BasicExpressionVisitor bev = new BasicExpressionVisitor(tuple);
		exp.accept(bev);
		boolean res = bev.getResult().getLast();
		return res;
	}

	/**
	 * check if the table all way down below this operator is empty or not.
	 * Only calls before the getNextTuple() to avoid the index being reset.
	 * 
	 * @return true if it is empty
	 */
	protected boolean isEmptyTable() {
		if (this.checkIfEmpty[0] == false) {
			this.checkIfEmpty[0] = true;
			// Only when first being checked will this.reset() being called and thus the index will not mess up
			this.reset();
			Tuple temp = this.getNextTuple();
			if (temp == null) {
				checkIfEmpty[1] = true;
			} else {
				this.reset();
				checkIfEmpty[1] = false;
			}
		} 
		return checkIfEmpty[1];
	}
	
	/**
	 * Write all Tuples to corresponding file
	 * 
	 * @param index index of file
	 * @throws IOException 
	 */
	public void dump(int index) throws IOException {
		reset();
		/*construct output path*/
		StringBuilder output = new StringBuilder(Dynamic_properties.outputPath);
		output.append("/query");	
		output.append(String.valueOf(index));
		
		TupleWriter write = new TupleWriter(output.toString());
		while (true) {
			Tuple tuple = getNextTuple();
			
			if (!write.writeTuple(tuple)) {
				break;
			}	
		}
		System.out.println("finish dumping");
		reset();
	}
		
		


}