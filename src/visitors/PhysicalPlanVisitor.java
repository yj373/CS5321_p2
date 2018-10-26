package visitors;

import java.util.LinkedList;
import java.util.List;

import logicalOperators.LogicalDuplicateEliminationOperator;
import logicalOperators.LogicalJoinOperator;
import logicalOperators.LogicalOperator;
import logicalOperators.LogicalProjectOperator;
import logicalOperators.LogicalScanOperator;
import logicalOperators.LogicalSortOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import operators.BNLJoinOperator;
import operators.DuplicateEliminationOperator;
import operators.JoinOperator;
import operators.Operator;
import operators.ProjectOperator;
import operators.SMJoinOperator;
import operators.ScanOperator;
import operators.SortOperator;
/**
 * Visit the logical plan and construct a physical operator 
 * query plan
 * @author yixuanjiang
 *
 */
public class PhysicalPlanVisitor {
	private Operator root;
	private LinkedList<Operator> childList;
	//Constructor
	public PhysicalPlanVisitor() {
		this.childList = new LinkedList<Operator>();
	}

	
	/**
	 * Get the physical query plan
	 * @return the root operator of the physical plan
	 */
	public Operator getPhysicalRoot() {
		return root;
	}
	
	/**
	 * Once visit a logical scan oprator, construct a 
	 * physical scan operator and add it to the child list
	 * @param scOp: logical scan operator
	 */
	public void visit(LogicalScanOperator scOp) {
		String tableName = scOp.getTableName();
		String tableAliase = scOp.getTableAliase();
		Expression expression = scOp.getCondition();
		ScanOperator scan = new ScanOperator(tableName, tableAliase, expression);
		childList.add(scan);
		root = scan;
	}
	
	/**
	 * Once visit a logical join operator, visit its left child and
	 * right child first. After the child list is updated, construct a 
	 * physical join operator. The first element of the child list is the 
	 * left child of the join operator, while the second element is
	 * the right child.
	 * @param jnOp: logical join operator
	 */
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
		Operator right = childList.pollLast();
		Operator left = childList.pollLast();
		
		
		//for yxx's test
		//JoinOperator join = new JoinOperator(left, right, exp);
		//JoinOperator join = new BNLJoinOperator(left, right, exp, 2);
		JoinOperator join = new SMJoinOperator(left, right, exp);
		
		
		
		
		childList.add(join);
		root = join;
	}
	
	/**
	 * Once visit a logical project operator, visit its left child. After
	 * the child list is updated, construct a physical project operator.
	 * Poll the last element in the child list as the left child of the project
	 * operator 
	 * @param operator: logical project operator
	 */
	public void visit(LogicalProjectOperator operator) {
		LogicalOperator op1 = operator.getLeftChild();
		if (op1 != null) {
			op1.accept(this);
		}
		List<SelectItem> sI = operator.getSelectItems();
		Operator left = childList.pollLast();
		ProjectOperator project = new ProjectOperator(sI, left);
		childList.add(project);
		root = project;
	}
	
	/**
	 * Once visit a logical sort operator, visit its left child. After
	 * the child list is updated, construct a physical sort operator.
	 * Poll the last element in the child list as the left child of the project
	 * operator 
	 * @param operator: logical sort operator
	 */
	public void visit(LogicalSortOperator operator) {
		LogicalOperator op1 = operator.getLeftChild();
		if (op1 != null) {
			op1.accept(this);
		}
		PlainSelect sI = operator.getPlainSelect();
		Operator left = childList.pollLast();
		SortOperator sort = new SortOperator(sI, left);
		childList.add(sort);
		root = sort;
	}
	
	/**
	 * Once visit a logical logical DuplicateElimination operator, visit its left child. After
	 * the child list is updated, construct a physical DuplicateElimination operator.
	 * Poll the last element in the child list as the left child of the project
	 * operator 
	 * @param operator: logical DuplicateElimination operator
	 */
	public void visit(LogicalDuplicateEliminationOperator operator) {
		LogicalOperator op1 = operator.getLeftChild();
		if (op1 != null) {
			op1.accept(this);
		}
		PlainSelect sI = operator.getPlainSelect();
		Operator left = childList.pollLast();
		DuplicateEliminationOperator distinct = new DuplicateEliminationOperator(sI, left);
		
		childList.add(distinct);
		root = distinct;
	}

}
