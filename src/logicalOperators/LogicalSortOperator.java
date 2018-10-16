package logicalOperators;

import net.sf.jsqlparser.statement.select.PlainSelect;
import visitors.LogicalPlanVisitor;
import visitors.PhysicalPlanVisitor;

public class LogicalSortOperator extends LogicalOperator{
	private PlainSelect plainSelect;	
	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 * 
	 * @param plainSelect  PlainSelect of query
	 * @param op  pass in child operator
	 * 
	 */
	public LogicalSortOperator(PlainSelect plainSelect, LogicalOperator op) {

		this.plainSelect = plainSelect;
		this.leftChild = op;
		this.rightChild = null;

		for (String tableAlias : op.getAllTable()) {
			this.allTable.add(tableAlias);
		}	
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
