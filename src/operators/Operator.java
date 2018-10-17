package operators;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;

import data.Dynamic_properties;
import data.Tuple;
import net.sf.jsqlparser.expression.Expression;
import util.TupleWriter;
import visitors.BasicExpressionVisitor;
import util.TupleWriter;
/**
 * parent class of every operator
 * 
 * @author Yixuan Jiang
 *
 */
public abstract class Operator {
	private Operator leftChild;
	private Operator rightChild;
	private Expression exp;

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
		reset();
		Tuple tuple = getNextTuple();
		while (tuple != null) {
			tuple.printData();
			tuple = getNextTuple();
		}
		reset();
	}

	
	/**以前的dump()
	 * Write all Tuples to corresponding file
	 * 
	 * @param index index of file
	 *

	public void dump(int index) {
		reset();
		String output_path = Dynamic_properties.outputPath;
		new File (output_path).mkdirs();
		File file = new File(output_path + "/query" + index);
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			Tuple tuple = getNextTuple();
			while (tuple != null) {
				tuple.printData();
				bw.write(tuple.getTupleData().toString() + '\n');
				tuple = getNextTuple();
			}
			bw.close();   
		}catch(IOException e) {
			e.printStackTrace();
			e.getMessage();
		}
		reset();
	}
	*/
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
		
		TupleWriter write = new TupleWriter(index);
		
		while (true) {
			Tuple tuple = getNextTuple();
			if (!write.writeTuple(index, tuple)) {
				break;
			}
		}
		reset();
	}
		
		
//		String output_path = Dynamic_properties.outputPath;
//		new File (output_path).mkdirs();
//		File file = new File(output_path + "/query" + index);
//		try {
//			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
//			Tuple tuple = getNextTuple();
//			while (tuple != null) {
//				tuple.printData();
//				bw.write(tuple.getTupleData().toString() + '\n');
//				tuple = getNextTuple();
//			}
//			bw.close();   
//		}catch(IOException e) {
//			e.printStackTrace();
//			e.getMessage();
//		}
//		reset();
//	}

}