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
	
	// extract the columns related to op1 from expression
	// extract the columns related to op2 from expression, corresponding to sequence of op1
	public SMJoinOperator(Operator op1, Operator op2, Expression expression, boolean isInMem) {
		super(op1, op2, expression);
		if (exp != null) {
			LocateExpressionVisitor locator = new LocateExpressionVisitor(op1.schema, op2.schema);
			exp.accept(locator);
			leftSortColumns = locator.leftSortColumns();
			rightSortColumns = locator.rightSortColumns();
			expList = locator.expList();
		}
	}

	// Set the sortOperator according to the config file;
	// Use in-mem-sort for current process.
	public void setSortOperator(boolean isInMem) {
		if (leftChild != null) {
			Operator originalLeft = leftChild;
			leftChild = new InMemSortOperator(originalLeft, leftSortColumns)			
		}
		if (rightChild != null) {
			Operator originalRight = rightChild;
			//rightChild = new InMemSortOperator(originalRight, rightSortColumns) :
			//	         new ExternalSortOperator(originalRight, rightSortColumns);			
		}
	}
	
	@Override()
	public Tuple getNextTuple() {
		return null;
	}
	
	
}
