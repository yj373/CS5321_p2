package operators;


import java.util.LinkedList;

import data.DataBase;
import data.OrderedTable;
import data.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import visitors.BasicExpressionVisitor;

/**
 * this class provides function:
 * handle Expression from WHERE and select required information
 * @author Yixuan Jiang
 *
 */

public class SelectOperator extends Operator{
	
	private Expression expression;
	private LinkedList<OrderedTable> tableAliase;
	private int expressionType;

	/**
	 * This method is to get the next tuple
	 * 
	 * @return next tuple
	 */
	@Override
	public Tuple getNextTuple() {
	
		Tuple t = getChild().getFirst().getNextTuple();
		if(expression!=null) {
			while (t!=null) {
				BasicExpressionVisitor bev = new BasicExpressionVisitor(t);
				expression.accept(bev);
				boolean res = bev.getResult().getLast();
				if(res) break;
				t = getChild().getFirst().getNextTuple();
			}
						
		}
		return t;
	}

	/**
	 * This method is to reset select operator
	 * by resetting its child operator
	 */
	@Override
	public void reset() {
		getChild().getFirst().reset();
		
	}
	
	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 * 
	 * @param plainSelect  PlainSelect of query
	 * @param op  pass in child operator
	 * 
	 */
	public SelectOperator(PlainSelect ps, Operator op) {
		String tableInfo = ps.getFromItem().toString();
		String[] aimTable = tableInfo.split("\\s+");
		if (aimTable.length<1) {
			return;
		}
		setChild(op);
		this.expression = ps.getWhere();
	}
	
	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 */
	
	public SelectOperator(OrderedTable tAliase, Expression ex, Operator op, int exType) {
		setChild(op);
		this.expression = ex;
		this.tableAliase = new LinkedList<OrderedTable>();
		this.tableAliase.add(tAliase);
		this.expressionType = exType;
	}
	
	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 */
	public SelectOperator(Expression ex, Operator op, int exType, LinkedList<OrderedTable> tAliase) {
		setChild(op);
		this.expression = ex;
		this.expressionType = exType;
		this.tableAliase = tAliase;
	}
	
	/**default constructor*/
	public SelectOperator() {
	}

	/** get expression*/
	public Expression getExpression() {
		return expression;
	}

	/** set expression*/
	public void setExpression(Expression expression) {
		this.expression = expression;
	}

	/** get table aliase*/
	public LinkedList<OrderedTable> getTableAliase() {
		return tableAliase;
	}

	/** set table aliase*/
	public void setTableAliase(OrderedTable tAliase) {
		LinkedList<OrderedTable> aliase = new LinkedList<OrderedTable>();
		aliase.add(tAliase);
		this.tableAliase = aliase;
	}

	/** get expression type*/
	public int getExpressionType() {
		return expressionType;
	}

	/** set expression type*/
	public void setExpressionType(int expressionType) {
		this.expressionType = expressionType;
	}

}