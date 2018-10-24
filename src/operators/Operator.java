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
	
	protected Map<String, Integer> schema;

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
			tuple.printData();
			tuple = getNextTuple();
		}
		reset();
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
		reset();
	}
		
		


}