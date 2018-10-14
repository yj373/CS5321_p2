package logicalOperators;

import java.util.*;

import visitors.BasicExpressionVisitor;
import visitors.LogicalPlanVisitor;
import data.Tuple;
import net.sf.jsqlparser.expression.Expression;

public abstract class LogicalOperator {
	protected LogicalOperator leftChild;
	protected LogicalOperator rightChild;
	protected Expression expression;
	protected Set<String> allTable = new HashSet<>();
	
	public boolean judgeExpression(Tuple tuple) {
		BasicExpressionVisitor bev = new BasicExpressionVisitor(tuple);
		expression.accept(bev);
		boolean res = bev.getResult().getLast();
		return res;
	}
	
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

}