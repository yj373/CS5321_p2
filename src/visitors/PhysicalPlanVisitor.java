package visitors;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import data.Dynamic_properties;
import logicalOperators.LogicalDuplicateEliminationOperator;
import logicalOperators.LogicalJoinOperator;
import logicalOperators.LogicalOperator;
import logicalOperators.LogicalProjectOperator;
import logicalOperators.LogicalScanOperator;
import logicalOperators.LogicalSortOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import operators.BNLJoinOperator;
import operators.DuplicateEliminationOperator;
import operators.ExternalSortOperator;
import operators.InMemSortOperator;
import operators.JoinOperator;
import operators.Operator;
import operators.ProjectOperator;
import operators.SMJoinOperator;
import operators.ScanOperator;
import operators.SortOperator;
import operators.V2ExternalSortOperator;
import util.GlobalLogger;
/**
 * Visit the logical plan and construct a physical operator 
 * query plan
 * @author yixuanjiang
 *
 */
public class PhysicalPlanVisitor {
	private Operator root;
	private LinkedList<Operator> childList;
	private int queryNum;
	private int joinType=0; // 0: TNLJ, 1: BNLJ, 2: SMJ
	private int sortType=0; // 0: in-memory, 1: external
	private int bnljBufferSize;
	private int exSortBufferSize;
	//Constructor
	public PhysicalPlanVisitor() {
		this.childList = new LinkedList<Operator>();
	}
	public PhysicalPlanVisitor(int qN) {
		this.childList = new LinkedList<Operator>();
		this.queryNum = qN;
		try {
			BufferedReader br = new BufferedReader(new FileReader(Dynamic_properties.configuePath));
			String line = br.readLine();
			String[] joinConfigue = line.split("\\s+");
			joinType = Integer.valueOf(joinConfigue[0]);
			if (joinConfigue.length==2) bnljBufferSize = Integer.valueOf(joinConfigue[1]);
			line = br.readLine();
			String[] sortConfigue = line.split("\\s+");
			sortType = Integer.valueOf(sortConfigue[0]);
			if (sortConfigue.length==2) exSortBufferSize = Integer.valueOf(sortConfigue[1]);
			br.close();
		}catch(IOException e) {
			GlobalLogger.getLogger().log(Level.SEVERE, e.toString(), e);
			
		}
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
	 * @throws Exception 
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
		
		if(joinType == 0) {
			JoinOperator join1 = new JoinOperator(left, right, exp);
			childList.add(join1);
			root = join1;
		}else if (joinType == 1) {
			BNLJoinOperator join2 = new BNLJoinOperator(left, right, exp, bnljBufferSize);
			childList.add(join2);
			root = join2;
		}else if (joinType == 2) {
			SMJoinOperator join3 = new SMJoinOperator(left, right, exp);
			if (sortType == 0) {
				if (left != null) {
					Operator originalLeft = left;
					left = new InMemSortOperator(originalLeft, join3.getLeftSortColumns());			                			
				}
				if (right != null) {
					Operator originalRight = right;
					right = new InMemSortOperator(originalRight, join3.getRightSortColumns());			
				}
			}else if(sortType == 1) {
				if (left != null) {
					Operator originalLeft = left;
					//left = new ExternalSortOperator(queryNum, exSortBufferSize, join3.getLeftSortColumns(), originalLeft.getSchema(), originalLeft);
					left = new V2ExternalSortOperator(queryNum, exSortBufferSize, join3.getLeftSortColumns(), originalLeft.getSchema(), originalLeft);
				}
				if (right != null) {
					Operator originalRight = right;
					//right = new ExternalSortOperator(queryNum, exSortBufferSize, join3.getRightSortColumns(), originalRight.getSchema(), originalRight);
					right = new V2ExternalSortOperator(queryNum, exSortBufferSize, join3.getRightSortColumns(), originalRight.getSchema(), originalRight);		

				}
			}
			join3.setLeftChild(left);
			join3.setRightChild(right);
			childList.add(join3);
			root = join3;
		}
		
	
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
		if (sortType == 0) {
			SortOperator sort1 = new SortOperator(sI, left);
			childList.add(sort1);
			root = sort1;
		}else if (sortType == 1) {
			List<OrderByElement> list = sI.getOrderByElements();
			List<String> attributes = new LinkedList<String>();
			if(list != null) {
				for (int i=0; i<list.size(); i++) {
					attributes.add(list.get(i).toString());
				}
			}
			V2ExternalSortOperator sort2;
			sort2 = new V2ExternalSortOperator(queryNum, exSortBufferSize, attributes, left.getSchema(), left);
			childList.add(sort2);
			root = sort2;
			
		}
		
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
