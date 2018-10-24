package operators;

import java.util.*;

import data.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import visitors.LocateExpressionVisitor;

public class SMJoinOperator extends JoinOperator{
	private List<String> leftSortColumns;
	private List<String> rightSortColumns;
	private List<Expression> expList;
	
	// Can I extends from JoinOperator? BNLJ may need this, too!
	private Tuple currLeftTup;
	private Tuple currRightTup;
	private Tuple rightPivot;
	
	// extract the columns related to op1 from expression
	// extract the columns related to op2 from expression, corresponding to sequence of op1
	public SMJoinOperator(Operator op1, Operator op2, Expression expression) {
		super(op1, op2, expression);
		if (exp != null) {
			LocateExpressionVisitor locator = new LocateExpressionVisitor(op1.schema, op2.schema);
			exp.accept(locator);
			leftSortColumns = locator.leftSortColumns();
			rightSortColumns = locator.rightSortColumns();
			expList = locator.expList();
			
			// Only when join condition exp is not null will we set 
			// the child to be in-mem-sort operator
			setSortOperator();
		}
	}

	// Set the sortOperator according to the config file;
	// Use in-mem-sort for current process.
	private void setSortOperator() {
		if (leftChild != null) {
			Operator originalLeft = leftChild;
			leftChild = new InMemSortOperator(originalLeft, leftSortColumns);			                			
		}
		if (rightChild != null) {
			Operator originalRight = rightChild;
			rightChild = new InMemSortOperator(originalRight, rightSortColumns);			
		}
	}
	
	
	// Only called after the sort operators having been set up
	@Override()
	public Tuple getNextTuple() {
		// if joinCondition is null, SMJ join is the same as TNLJ.
		if (exp == null) {
			return super.getNextTuple();
		}
		// as join condition is not null, as long as there is one empty table, return null;
		if (leftChild.isEmptyTable() || rightChild.isEmptyTable()) {
			return null;
		}
		if (currLeftTup == null && currRightTup == null) {
			currLeftTup = leftChild.getNextTuple();
			currRightTup = rightChild.getNextTuple();
		}
      		
		/* If currLeftTup and currRightTup are both null, it is the start of join
		 *  If currLeftTup is null but currRightTup is not null, it is the end of join 
		 */
		while (currLeftTup != null && currRightTup != null) {
			while (compareBtwnTable(currLeftTup, currRightTup) > 0) {
				currRightTup = rightChild.getNextTuple();
				// what if currRight == null ?
			}
			while (compareBtwnTable(currLeftTup, currRightTup) < 0) {
			    currLeftTup = leftChild.getNextTuple();
			    // what if currLeft == null ?
			}
			rightPivot = currRightTup;
			while (compareBtwnTable(currLeftTup, currRightTup) == 0) {
				currRightTup = rightPivot;
				while (compareBtwnTable(currLeftTup, currRightTup) == 0) {
					
				}
			}
			currRightTup = rightPivot;
		}
		return null;
	}
	
	// differing from the compare function in sortOperator, in this function we compare
	// two tuples from two tables with different schemas.
	// o1: must be the tuple from LEFT table;
	// o2: must be the tuple from RIGHT table;
	private int compareBtwnTable(Tuple o1, Tuple o2) {
		if (leftSortColumns != null && rightSortColumns != null) {
			for (int i = 0; i < leftSortColumns.size(); i++) {
				Integer leftCol = leftChild.schema.get(leftSortColumns.get(i));
				Integer rightCol = rightChild.schema.get(rightSortColumns.get(i));
				if (o1.getData()[leftCol] < o2.getData()[rightCol]) {
					return -1;
				} 
				if (o1.getData()[leftCol] > o2.getData()[rightCol]){
					return 1;
				} 
			}
			return 0;
		}
		/* if sortColumns is null, say there is no join condition in query,
		 * return tuple directly without sorting.
		 */
		return 0;
	}

}
