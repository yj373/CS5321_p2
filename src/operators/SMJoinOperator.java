package operators;

import java.util.*;

import data.Tuple;
import net.sf.jsqlparser.expression.Expression;
import visitors.LocateExpressionVisitor;

public class SMJoinOperator extends JoinOperator{
	private List<String> leftSortColumns;
	private List<String> rightSortColumns;
	
	
	// Can I extends from JoinOperator? BNLJ may need this, too!
	private Tuple currLeftTup;
	private Tuple currRightTup;
	
	/* the marker of reset position in right table 
	 * pivot is -1 when it is not in the process of merging. 
	 * Once the first equal entry pair is found, pivot is set to be rightIdx and 
	 * remains unchanged in the following merging of equal entry pairs; Then when 
	 * the first unequal pair is found, reset right table by pivot and set pivot to -1 then.*/
	private int pivot = -1;
	private int rightIdx = -1; // The index of currRightTup in right table, -1 when getNextTup() is not called.
	
	// extract the columns related to op1 from expression
	// extract the columns related to op2 from expression, corresponding to sequence of op1
	public SMJoinOperator(Operator op1, Operator op2, Expression expression) {
		super(op1, op2, expression);
		StringBuilder sb = new StringBuilder();
		sb.append("smj-");
		sb.append(op1.name);
		sb.append("-");
		sb.append(op2.name);
		name = sb.toString();
		if (exp != null) {
			LocateExpressionVisitor locator = new LocateExpressionVisitor(op1.schema, op2.schema);
			exp.accept(locator);
			leftSortColumns = locator.leftSortColumns();
			rightSortColumns = locator.rightSortColumns();
			
			// Only when join condition exp is not null will we set 
			// the child to be in-mem-sort operator
			//setSortOperator();
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
		/* corner case 1: if joinCondition is null, SMJ join is the same as TNLJ. */
		if (exp == null) {
			return super.getNextTuple();
		}
		
		/* corner case 2: Since join condition is not null, as long as there 
		 * is one empty table, return null */
		if (leftChild.isEmptyTable() || rightChild.isEmptyTable()) {
			return null;
		}
		
		/* corner case 3: beginning of the merge process */
		if (currLeftTup == null && currRightTup == null) {
			currLeftTup = leftChild.getNextTuple();
			currRightTup = rightChild.getNextTuple();
			rightIdx ++;
		}
		
		/* corner case 4: reach the end of the merge process directly when leftTable reaches end;
		 * for right table, it may need to be reset to pivot when it returns null tuple*/
		if (currLeftTup == null) {
			return null;
		}

		/* When currLeftTup != null && currRightTup != null */

		// pivot is the marker of reset position.
		// If we do not need to reset Right partition scan yet, pivot is -1.
		if (pivot < 0) {
			int comprRes = 0;
			while ((currLeftTup != null && currRightTup != null) &&
					(comprRes = compareBtwnTable(currLeftTup, currRightTup)) != 0) {
				if (comprRes > 0) {
					currRightTup = rightChild.getNextTuple();
					rightIdx ++;
				} else {
					currLeftTup = leftChild.getNextTuple();
				}
			}
			if (currLeftTup == null || currRightTup == null) {
				return null;
			}
			// At this point currRightTup must equalsTo currRightTup, update the pivot
			pivot = rightIdx;
			Tuple result = concatenate(currLeftTup, currRightTup);
			currRightTup = rightChild.getNextTuple();
			rightIdx ++;
			return result;
		}			
		// pivot is non-negative, which means we need to reset RightTable by index if needed
		
		// In the case below, the case of currLeft == null has been ruled out at the very beginning in corner case 4
		// Therefore we only need to consider if currRightTup == null
		if (currRightTup != null && compareBtwnTable(currLeftTup, currRightTup) == 0) {
			Tuple result = concatenate(currLeftTup, currRightTup);
			currRightTup = rightChild.getNextTuple();
			rightIdx ++;
			return result;
			// reset the rightTable, after the reset, pivot is -1 again because we do not need reset yet.
		} else {
			currLeftTup = leftChild.getNextTuple();
			rightChild.reset(pivot);
			currRightTup = rightChild.getNextTuple();
			rightIdx = pivot;
			pivot = -1;
			return this.getNextTuple();
		}

	}
	
	// DIFFERENT from the compare function in sortOperator, in this function we compare
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
	public List<String> getLeftSortColumns(){
		return this.leftSortColumns;
	}
	public List<String> getRightSortColumns(){
		return this.rightSortColumns;
	}
	

}