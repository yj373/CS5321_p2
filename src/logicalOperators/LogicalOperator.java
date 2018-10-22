package logicalOperators;

import java.util.*;

import visitors.LogicalPlanVisitor;
import visitors.PhysicalPlanVisitor;
import net.sf.jsqlparser.expression.Expression;
/**
 * The base class of logical operators.
 * @author Ruoxuan Xu
 *
 */
public abstract class LogicalOperator {
	protected LogicalOperator leftChild;
	protected LogicalOperator rightChild;
	protected Expression expression;
	protected Set<String> allTable = new HashSet<>();
	
	
	public void setExpression(Expression ex) {
		expression = ex;
	}
	
	public Expression getCondition() {
		return expression;
	}
	
	public void setLeftChild(LogicalOperator left) {
		leftChild = left;
	}
	
	public void setRightChild(LogicalOperator right) {
		rightChild = right;
	}
	
	public LogicalOperator getLeftChild() {
		return leftChild;
	}
	
	public LogicalOperator getRightChild() {
		return rightChild;
	}
	
	// Table aliases that are enclosed by this operator
	public Set<String> getAllTable() {
		return allTable;
	}
	
	// this should be an abstract method, or the JoinLogicalOperator has to implement visit(LogicalOperator op)
	public abstract void accept(LogicalPlanVisitor visitor);
	public abstract void accept(PhysicalPlanVisitor visitor);

}