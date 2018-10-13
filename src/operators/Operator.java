package operators;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;

import data.Dynamic_properties;
import data.Tuple;

/**
 * parent class of every operator
 * 
 * @author Yixuan Jiang
 *
 */
public abstract class Operator {
	private Operator parent;
	private LinkedList<Operator> child;

	/**
	 * get a linkedlist of child operator
	 */
	public LinkedList<Operator> getChild() {
		return child;
	}

	/**
	 * set child operator
	 */
	public void setChild(LinkedList<Operator> child) {
		this.child = child;
	}
	
	/**
	 * set child operator
	 */
	public void setChild(Operator op) {
		LinkedList<Operator> childList = new LinkedList<Operator>();
		childList.add(op);
		this.child = childList;
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

	/**
	 * Write all Tuples to corresponding .txt file
	 * 
	 * @param index index of .txt file
	 */

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

	/**
	 * getter method to get parent operator
	 */
	public Operator getParent() {
		return parent;
	}

	/**
	 * setter method to set parent operator
	 */
	public void setParent(Operator parent) {
		this.parent = parent;
	}


}