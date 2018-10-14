package logicalOperators;

import data.Tuple;
import net.sf.jsqlparser.statement.select.PlainSelect;
import visitors.LogicalPlanVisitor;

public class LogicalDuplicateEliminationOperator extends LogicalOperator{
	private Tuple prevTuple;
	/*check DISTINCT keyword*/
	boolean workState; 

	/** 
	 * This method is a constructor which is to
	 * check the DISTINCT key word in the query and then
	 * initialize related fields
	 * 
	 * @param plainSelect  PlainSelect of query
	 * @param op  pass in child operator
	 * 
	 */
	

	public LogicalDuplicateEliminationOperator(PlainSelect ps, LogicalOperator op) {
		if (ps.getDistinct()!=null) {
			workState = true;
		}else {
			workState = false;
		}
		this.leftChild = op;
		this.rightChild = null;
	}
	
	public boolean getWorkState() {
		return workState;
	}

	@Override
	public void accept(LogicalPlanVisitor visitor) {
		visitor.visit(this);
	}

}
