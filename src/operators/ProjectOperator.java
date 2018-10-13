package operators;


import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import data.Tuple;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * This class provides function:
 * Choose required attributes according 
 * to the SELECT requirement
 * 
 * @author Xiaoxing Yan
 */
public class ProjectOperator extends Operator{
	
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

	public ProjectOperator(PlainSelect plainSelect,Operator op) {
		LinkedList<Operator> newChild = new LinkedList<Operator>();
		newChild.add(op);
		super.setChild(newChild);
		selectItems = plainSelect.getSelectItems();
		if (selectItems.get(0).toString() == "*") {
			allColumns = true;
		} 
		
	}

	
	/**
	 * This method is to get the next tuple after projection
	 * 
	 * @return next tuple after projection
	 */
	@Override
	public Tuple getNextTuple() {
		Operator child = getChild().get(0);
		Tuple current = child.getNextTuple();
		if (current != null && !allColumns) {
			/*Assume there must be corresponding columns in the given tuple*/
			long[] data = new long[selectItems.size()];
			Map<String, Integer> schema = new HashMap<String, Integer>();
			int index = 0;
			boolean flag = false;
			for (SelectItem expre : selectItems) {
				String attributeName = expre.toString();
				Integer dataIndex = current.getSchema().get(attributeName);
				if (dataIndex!=null) {
					flag = true;
					data[index] = current.getData()[dataIndex];
					schema.put(attributeName, index);
					index++;
				}	
			}
			if (flag) {
				current = new Tuple(data, schema);
			}else {
				current = null;
			}
			
		}
		return current;
	}

	/**
	 * This method is to reset project operator
	 * by resetting its child operator
	 */
	@Override
	public void reset() {
		getChild().get(0).reset();
		
	}
	
}