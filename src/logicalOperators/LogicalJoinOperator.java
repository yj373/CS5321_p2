package logicalOperators;

import visitors.LogicalPlanVisitor;
import visitors.PhysicalPlanVisitor;

/**
 * This class is a logical join operator. It has left child and right child as its child 
 * logical operators, and also a field of table list to track all table aliases involved 
 * in this join operator. An join operator will be initialized in LogicalPlanBuiler, and
 * later will be visited by PhysicalPlanVisitor to generate the physical plan. 
 * 
 * @author Ruoxuan Xu
 *
 */
public class LogicalJoinOperator extends LogicalOperator {
    /**
     * Construct a LogicalJoinOperator instance, with its left child to be op1, and 
     * right child to be op2.
     * @param op1
     * @param op2
     */
	public LogicalJoinOperator(LogicalOperator op1, LogicalOperator op2) {
		if (op1 != null) {
			for (String e : op1.getAllTable()) {
				this.allTable.add(e);
			}
		}
		if (op2 != null) {
			for (String e : op2.getAllTable()) {
				this.allTable.add(e);
			}
		}
    	leftChild = op1;
    	rightChild = op2;
	}
	
	/**
	 * Accept the visitor of logicalPlanVisitor
	 */
	@Override
	public void accept(LogicalPlanVisitor visitor) {
		visitor.visit(this);
	}
	
	/**
	 * get LeftChild Operator
	 * @return the leftChild
	 */
	@Override
	public LogicalOperator getLeftChild() {
		return leftChild;
	}
	
	/**
	 * get rightChild Operator
	 * @return the rightChild
	 */
	@Override
	public LogicalOperator getRightChild() {
		return rightChild;
	}

	/**
	 * Accept the visitor of PhysicalPlanVisitor
	 */
	@Override
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
		
	}

}
