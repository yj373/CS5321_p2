package operators;

import java.util.*;

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
	public SMJoinOperator(Operator op1, Operator op2, Expression expression) {
		super(op1, op2, expression);
		if (exp != null) {
			LocateExpressionVisitor locator = new LocateExpressionVisitor(op1.schema, op2.schema);
			exp.accept(locator);
			leftSortColumns = locator.leftSortColumns();
			rightSortColumns = locator.rightSortColumns();
			expList = locator.expList();
		}
	}

	public void setSortOperator() {
		if (leftChild != null) {
			Operator originalLeft = leftChild;
			leftChild = new InMemSortOperator(originalLeft, leftSortColumns);
			
		}
	}
	
	
}
