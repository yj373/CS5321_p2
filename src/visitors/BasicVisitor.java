package visitors;


import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

import data.OrderedTable;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;
import operators.DuplicateEliminationOperator;
import operators.JoinOperator;
import operators.Operator;
import operators.ProjectOperator;
import operators.ScanOperator;
import operators.SelectOperator;
import operators.SortOperator;

/**
 * this class provides function:
 * build query plan
 * 
 * @author Yixuan Jiang
 *
 */
public class BasicVisitor implements SelectVisitor, FromItemVisitor, ItemsListVisitor, ExpressionVisitor{
	
	
	private Operator rootOp = null;
	private Deque<Operator> joinStack = new LinkedList<Operator>() ;
	private Map<OrderedTable, Operator[]> tableDirectory = new HashMap<>();
	private LinkedList<OrderedTable> currentTable = new LinkedList<>();
	private LinkedList<OrderedTable> joinedTables = new LinkedList<OrderedTable>();
	
	/**
	 * Get the query plan which is a tree of operators
	 * @param select: result of JsqlParser
	 * @return the root operator
	 */
	public Operator getQueryPlan(Select select) {
		select.getSelectBody().accept(this);
		return rootOp;
	}

	/**
	 * The visitor visits a PlainSelect
	 * @param ps: result of JsqlParser
	 *
	 */
	public void visit(PlainSelect ps) {
		ps.getFromItem().accept(this);
		if (ps.getJoins()!=null) {
			for(int i = 0; i < ps.getJoins().size(); i++) {
				Object j = ps.getJoins().get(i);
				String tableInfo = j.toString();
				ScanOperator scanOp = new ScanOperator(tableInfo);

				Operator[] rootEnd = new Operator[2];
				rootEnd[0] = scanOp;
				rootEnd[1] = scanOp;
				
				tableDirectory.put(new OrderedTable(scanOp.getTableAliase(), i+1), rootEnd);
 
			}
		}

		if (ps.getWhere() != null) {
			ps.getWhere().accept(this);
		}
		HashSet<Operator> operatorChecker = new HashSet<Operator>();
		for(OrderedTable s : tableDirectory.keySet()) {
			Operator candiOp = tableDirectory.get(s)[1];
			if (!operatorChecker.contains(candiOp)) {
				joinStack.addFirst(candiOp);;
				operatorChecker.add(candiOp);
			}
		}
		
		while (joinStack.size() > 1) {
			Operator right = joinStack.pop();
			Operator left = joinStack.pop();
			JoinOperator newJoin = new JoinOperator(left, right);
			joinStack.addFirst(newJoin);
		}
		Operator finalJoinOperator = joinStack.pop();
		ProjectOperator proOp = new ProjectOperator(ps, finalJoinOperator);
		finalJoinOperator.setParent(proOp);
		SortOperator sortOp = new SortOperator(ps, proOp);
		proOp.setParent(sortOp);
		DuplicateEliminationOperator dOp= new DuplicateEliminationOperator(ps, sortOp);
		sortOp.setParent(dOp);
		rootOp = dOp;
	}
	
	/**
	 * The visitor visits a table
	 * @param table: result of JsqlParser
	 */
	public void visit(Table table) {
		ScanOperator scanOp = new ScanOperator(table.getWholeTableName(), table.getAlias());
		Operator[] rootEnd = new Operator[2];
		rootEnd[0] = scanOp;
		rootEnd[1] = scanOp;

		tableDirectory.put(new OrderedTable(scanOp.getTableAliase(), 0), rootEnd);		
		
	}
	
	@Override
	/**
	 * The visitor visits a column
	 * @param column: result of JsqlParser
	 */
	public void visit(Column column) {
		String[] tableNameIndices = column.getWholeColumnName().split("\\.");
		String tableAlias = tableNameIndices[0];
		OrderedTable tableNameIndex = null;
		for (OrderedTable s : tableDirectory.keySet()) {
			if (s.getTableAliase().equals(tableAlias)) {
				tableNameIndex = s;
			}
		}	
		currentTable.add(tableNameIndex);
		
	}
	
	@Override
	/**
	 * The visitor visits a LongValue
	 * @param lonV: result of JsqlParser
	 */
	public void visit(LongValue longV) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	/**
	 * The visitor visits an AndExpression
	 * @param and: result of JsqlParser
	 */
	public void visit(AndExpression and) {
		and.getLeftExpression().accept(this);
		and.getRightExpression().accept(this);
		
	}
	
	@Override
	/**
	 * The visitor visits an EqualsTo
	 * @param equals: result of JsqlParser
	 */
	public void visit(EqualsTo equals) {
		Expression left = equals.getLeftExpression();
		Expression right = equals.getRightExpression();
		int expressionType = getExpressionType(left, right);
		left.accept(this);
		right.accept(this);
		if (expressionType == 1) {
			OrderedTable tableNameIndex = currentTable.getLast();
			currentTable.removeLast();
			addSelectOprator(tableNameIndex, equals, expressionType);
		}else {
			OrderedTable table2 = currentTable.getLast();
			currentTable.removeLast();
			OrderedTable table1 = currentTable.getLast();
			currentTable.removeLast();
			addJoinOperator(table1, table2, equals, expressionType);
		}
				
	}

	@Override
	/**
	 * The visitor visits a GreaterThan
	 * @param greater: result of JsqlParser
	 */
	public void visit(GreaterThan greater) {
		Expression left = greater.getLeftExpression();
		Expression right = greater.getRightExpression();
		int expressionType = getExpressionType(left, right);
		left.accept(this);
		right.accept(this);
		if (expressionType == 1) {
			OrderedTable tableNameIndex = currentTable.getLast();
			currentTable.removeLast();
			addSelectOprator(tableNameIndex, greater, expressionType);
		}else {
			OrderedTable table2 = currentTable.getLast();
			currentTable.removeLast();
			OrderedTable table1 = currentTable.getLast();
			currentTable.removeLast();
			addJoinOperator(table1, table2, greater, expressionType);
		}
		
	}

	@Override
	/**
	 * The visitor visits a column
	 * @param column: result of JsqlParser
	 */
	public void visit(GreaterThanEquals greaterEquals) {
		Expression left = greaterEquals.getLeftExpression();
		Expression right = greaterEquals.getRightExpression();
		int expressionType = getExpressionType(left, right);
		left.accept(this);
		right.accept(this);
		if (expressionType == 1) {
			OrderedTable tableNameIndex = currentTable.getLast();
			currentTable.removeLast();
			addSelectOprator(tableNameIndex, greaterEquals, expressionType);
		}else {
			OrderedTable table2 = currentTable.getLast();
			currentTable.removeLast();
			OrderedTable table1 = currentTable.getLast();
			currentTable.removeLast();
			addJoinOperator(table1, table2, greaterEquals, expressionType);
		}
		
	}
	
	@Override
	/**
	 * The visitor visits a MinorThan
	 * @param minor: result of JsqlParser
	 */
	public void visit(MinorThan minor) {
		Expression left = minor.getLeftExpression();
		Expression right = minor.getRightExpression();
		int expressionType = getExpressionType(left, right);
		left.accept(this);
		right.accept(this);
		if (expressionType == 1) {
			OrderedTable tableNameIndex = currentTable.getLast();
			currentTable.removeLast();
			addSelectOprator(tableNameIndex, minor, expressionType);
		}else {
			OrderedTable table2 = currentTable.getLast();
			currentTable.removeLast();
			OrderedTable table1 = currentTable.getLast();
			currentTable.removeLast();
			addJoinOperator(table1, table2, minor, expressionType);
	
		}
		
	}

	@Override
	/**
	 * The visitor visits a MinorThanEquals
	 * @param minorequals: result of JsqlParser
	 */
	public void visit(MinorThanEquals minorEquals) {
		Expression left = minorEquals.getLeftExpression();
		Expression right = minorEquals.getRightExpression();
		int expressionType = getExpressionType(left, right);
		left.accept(this);
		right.accept(this);
		if (expressionType == 1) {
            OrderedTable tableNameIndex = currentTable.getLast();
			currentTable.removeLast();
			addSelectOprator(tableNameIndex, minorEquals, expressionType);
		}else {
			OrderedTable table2 = currentTable.getLast();
			currentTable.removeLast();
			OrderedTable table1 = currentTable.getLast();
			currentTable.removeLast();
			addJoinOperator(table1, table2, minorEquals, expressionType);

		}
		
	}

	@Override
	/**
	 * The visitor visits a NotEqualsTo
	 * @param notEquals: result of JsqlParser
	 */
	public void visit(NotEqualsTo notEquals) {
		Expression left = notEquals.getLeftExpression();
		Expression right = notEquals.getRightExpression();
		int expressionType = getExpressionType(left, right);
		left.accept(this);
		right.accept(this);
		if (expressionType == 1) {
			OrderedTable tableNameIndex = currentTable.getLast();
			currentTable.removeLast();
			addSelectOprator(tableNameIndex, notEquals, expressionType);
		}else {

			OrderedTable rightTable = currentTable.getLast();
			currentTable.removeLast();
			OrderedTable leftTable = currentTable.getLast();
			currentTable.removeLast();
			addJoinOperator(leftTable, rightTable, notEquals, expressionType);
		}
		
	}
	
	/**
	 * Classify the expressions
	 * Type 1: don't need join, Type 2: need join
	 * @param e1: candidate expression 1
	 * @param e2: candidate expression 2
	 * @return the type of this expression
	 */
	private int getExpressionType(Expression e1, Expression e2) {
		if (e1 instanceof Column && e2 instanceof Column) return 2;
		return 1;
	}
	/**
	 * Once read in a type 1 expression, a select operator needs to be added to
	 * the operator tree. The new SelectOperator needs to be added right above the 
	 * ScamOperator of corresponding table
	 * @param tableNameIndex: the table that needs a select operator
	 * @param ex: the expression needs to be stored
	 * @param exType: the type of the expression
	 */
	private void addSelectOprator(OrderedTable tableNameIndex, Expression ex, int exType) {
		Operator root = tableDirectory.get(tableNameIndex)[0];
		Operator end = tableDirectory.get(tableNameIndex)[1];
		SelectOperator selectOp = new SelectOperator(tableNameIndex, ex, root, exType);
		if (root.equals(end)) {

			Operator[] newValue = tableDirectory.get(tableNameIndex);
			root.setParent(selectOp);
			newValue[1] = selectOp;			
			tableDirectory.put(tableNameIndex, newValue);
			
		} else {
			// If the parent operator of the root node is a JoinOperator
			if (root.getParent() instanceof JoinOperator) {
				if (root.equals(root.getParent().getChild().get(0))) {
					// If the root node is the left child of the JoinOperator
					updateChildParentState(root, selectOp, 0);
				}else {
					// If the root node is the right child of the JoinOperator
					updateChildParentState(root, selectOp, 1);
				}
			}else {
				// The parent operator will only has one child
				updateChildParentState(root, selectOp, 0);				
			}
		}
		
	}
	/**
	 * update the child parent state when a SelectOperator needs to be added
	 * @param root: the ScanOperator of the corresponding table
	 * @param newOp: the new SelectOperator that needs to be added to the tree
	 * @param ind: determine how to update the childList of the parent operator of the root
	 */
	private void updateChildParentState(Operator root, Operator newOp, int ind) {
		LinkedList<Operator> newChild = root.getParent().getChild();
		newChild.remove(ind);
		if (ind == 0) {
			newChild.addFirst(newOp);
		} else {
			newChild.add(newOp);
		}
		
		root.getParent().setChild(newChild);
		newOp.setParent(root.getParent());
		root.setParent(newOp);
		
	}
	
	

	/**
	 * Once read in a type 2 expression, a JoinOperator and a SelectOperator need to be added to
	 * the operator tree. The new JoinOperator needs to add the top operators of its left child table and 
	 * the right child table. The SelectOperator is the parent operator of the JoinOperator.
	 * @param table1: the left child target table
	 * @param table2: the right child target table
	 * @param ex: the expression stored in the SelectOperator
	 * @param exType: the type of the expression
	 */
	private void addJoinOperator(OrderedTable table1, OrderedTable table2, Expression ex, int exType) {
		if (table1.getIndex() > table2.getIndex()) {
			OrderedTable temp = table1;
			table1 = table2;
			table2 = temp;
		}
		
		JoinOperator joinOp = new JoinOperator(tableDirectory.get(table1)[1], tableDirectory.get(table2)[1]);
		joinedTables.add(table1);
		joinedTables.add(table2);
		LinkedList<OrderedTable> tAliase = new LinkedList<OrderedTable>();
		tAliase.add(table1);
		tAliase.add(table2);
		SelectOperator selectOp = new SelectOperator(ex, joinOp, exType, tAliase);
		joinOp.setParent(selectOp);
		
		updateChildParentState2(table1, joinOp, selectOp);
		updateChildParentState2(table2, joinOp, selectOp);
		
		//If any child operator of the 
		if (tableDirectory.get(table1)[1] instanceof SelectOperator) {
			SelectOperator temp = (SelectOperator) tableDirectory.get(table1)[1];
			if(temp.getExpressionType()!=1) {
				for (OrderedTable ot: joinedTables) {
					updateChildParentState2(ot, joinOp, selectOp);
				}
				
			}
		}else if (tableDirectory.get(table2)[1] instanceof SelectOperator) {
			SelectOperator temp = (SelectOperator) tableDirectory.get(table2)[1];
			if(temp.getExpressionType()!=1) {
				for (OrderedTable ot: joinedTables) {
					updateChildParentState2(ot, joinOp, selectOp);	
				}
				
			}
		}
	}
	private void updateChildParentState2(OrderedTable table, Operator joinOp, Operator selectOp) {
		Operator[] newValue = tableDirectory.get(table);
		newValue[1].setParent(joinOp);
		newValue[1] = selectOp;
		tableDirectory.put(table, newValue);
		
	}
	
	@Override
	public void visit(ExpressionList expressionList) {
		// TODO Auto-generated method stub
		
	}
	
	public void visit(Union arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	public void visit(SubJoin arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Function arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub
		
	}

	

	@Override
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub
		
	}

}

