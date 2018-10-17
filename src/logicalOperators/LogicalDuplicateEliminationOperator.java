package logicalOperators;

import data.Tuple;
import net.sf.jsqlparser.statement.select.PlainSelect;
import visitors.LogicalPlanVisitor;
import visitors.PhysicalPlanVisitor;

/**
 * This class is a logical DuplicateElimination operator to store DISTINCT info from
 * JSql parser.
 * 
 * @author Ruoxuan Xu
 */
public class LogicalDuplicateEliminationOperator extends LogicalOperator{
	private Tuple prevTuple;
	private PlainSelect plainSelect;
	/*check DISTINCT keyword*/
	boolean workState; 

	/** 
	 * This method is a constructor which is to
	 * check the DISTINCT key word in the query and then
	 * initialize related fields
	 * 
	 * @param plainSelect  PlainSelect of query
	 * @param op  set as left child operator
	 * 
	 */
	

	public LogicalDuplicateEliminationOperator(PlainSelect ps, LogicalOperator op) {
		if (ps.getDistinct() != null) {
			workState = true;
		} else {
			workState = false;
		}
		this.leftChild = op;
		this.rightChild = null;
		this.plainSelect = ps;
	}
	
	public boolean getWorkState() {
		return workState;
	}
	public PlainSelect getPlainSelect() {
		return plainSelect;
	}

	@Override
	public void accept(LogicalPlanVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
	}

}
