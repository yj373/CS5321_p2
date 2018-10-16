package logicalOperators;

import visitors.LogicalPlanVisitor;
import visitors.PhysicalPlanVisitor;

public class LogicalJoinOperator extends LogicalOperator {

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
	
	@Override
	public void accept(LogicalPlanVisitor visitor) {
		visitor.visit(this);
	}
	
	/**
	 * get LeftChild Operator
	 * @return the leftChild
	 */
	public LogicalOperator getLeftChild() {
		return leftChild;
	}
	
	/**
	 * get rightChild Operator
	 * @return the rightChild
	 */
	public LogicalOperator getRightChild() {
		return rightChild;
	}

	@Override
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
		
	}

}
