package logicalOperators;

import java.util.List;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import visitors.LogicalPlanVisitor;
import visitors.PhysicalPlanVisitor;

public class LogicalProjectOperator extends LogicalOperator{
	/*store information of needed attributes*/
	private List<SelectItem> selectItems;
	/*check that whether return all attributes or return specific attributes*/
	private boolean allColumns = false;
	
	/** 
	 * This method is a constructor which is to
	 * get corresponding columns information and initialize childOp.
	 * 
	 * @param plainSelect  PlainSelect of query
	 * @param op  pass in child operator
	 * 
	 */

	public LogicalProjectOperator(PlainSelect plainSelect,LogicalOperator op) {
		this.leftChild = op;
		this.rightChild = null;
		for (String e : op.getAllTable()) {
			allTable.add(e);
		}
		selectItems = plainSelect.getSelectItems();
		if (selectItems.get(0).toString() == "*") {
			allColumns = true;
		} 		
	}
	
	public boolean isAllCol() {
		return allColumns;
	}
	
	public List<SelectItem> getSelectItems() {
		return selectItems;
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
