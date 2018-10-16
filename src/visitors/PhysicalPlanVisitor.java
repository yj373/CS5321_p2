package visitors;

import java.util.LinkedList;
import java.util.List;

import data.TablePair;
import logicalOperators.LogicalDuplicateEliminationOperator;
import logicalOperators.LogicalJoinOperator;
import logicalOperators.LogicalOperator;
import logicalOperators.LogicalProjectOperator;
import logicalOperators.LogicalScanOperator;
import logicalOperators.LogicalSortOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import operators.JoinOperator;
import operators.Operator;
import operators.ProjectOperator;
import operators.ScanOperator;
import operators.SortOperator;

public class PhysicalPlanVisitor {
	private Operator root;
	private LinkedList<Operator> childList;
	
	public PhysicalPlanVisitor() {
		this.childList = new LinkedList<Operator>();
	}


	public Operator getPhysicalRoot() {
		return root;
	}
	
	public void visit(LogicalScanOperator scOp) {
		String tableName = scOp.getTableName();
		String tableAliase = scOp.getTableAliase();
		Expression expression = scOp.getCondition();
		ScanOperator scan = new ScanOperator(tableName, tableAliase, expression);
		childList.add(scan);
		root = scan;
	}
	
	public void visit(LogicalJoinOperator jnOp) {
		LogicalOperator op1 = jnOp.getLeftChild();
		if (op1 != null) {
			op1.accept(this);
		}
		LogicalOperator op2 = jnOp.getRightChild();
		if (op2 != null) {
			op2.accept(this);
		}
		Expression exp = jnOp.getCondition();
		Operator left = childList.pollFirst();
		Operator right = childList.pollFirst();
		JoinOperator join = new JoinOperator(left, right, exp);
		childList.add(join);
		root = join;
	}
	
	public void visit(LogicalProjectOperator operator) {
		LogicalOperator op1 = operator.getLeftChild();
		if (op1 != null) {
			op1.accept(this);
		}
		List<SelectItem> sI = operator.getSelectItems();
		Operator left = childList.pollFirst();
		ProjectOperator project = new ProjectOperator(sI, left);
		childList.add(project);
		root = project;
	}
	
	public void visit(LogicalSortOperator operator) {
		LogicalOperator op1 = operator.getLeftChild();
		if (op1 != null) {
			op1.accept(this);
		}
		PlainSelect sI = operator.getPlainSelect();
		Operator left = childList.pollFirst();
		SortOperator sort = new SortOperator(sI, left);
		childList.add(sort);
		root = sort;
	}

	public void visit(LogicalDuplicateEliminationOperator operator) {
		LogicalOperator op1 = operator.getLeftChild();
		if (op1 != null) {
			op1.accept(this);
		}
		PlainSelect sI = operator.getPlainSelect();
		Operator left = childList.pollFirst();
		ProjectOperator distinct = new ProjectOperator(sI, left);
		childList.add(distinct);
		root = distinct;
	}

}
