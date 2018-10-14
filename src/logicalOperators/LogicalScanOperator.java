package logicalOperators;

import java.util.LinkedList;

import data.DataBase;
import visitors.LogicalPlanVisitor;
import visitors.PhysicalPlanVisitor;

public class LogicalScanOperator extends LogicalOperator {
	private String tableName;
	private String tableAddress;
	private String tableAliase;
	private LinkedList<String> attributes;
	
	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 * 
	 * @param tableInfo table information
	 * 
	 */
	public LogicalScanOperator(String tableInfo) {
		String[] aimTable = tableInfo.split("\\s+");
		if (aimTable.length<1) {
			this.tableName = null;
			return;
		}
		this.tableName = aimTable[0];
		this.tableAddress = DataBase.getInstance().getAddresses(tableName);
		this.tableAliase = aimTable[aimTable.length-1];
		this.attributes = DataBase.getInstance().getSchema(tableName);
		this.allTable.add(tableAliase);
	}
	
	@Override
	public void accept(LogicalPlanVisitor visitor) {
		visitor.visit(this);
	}
	
	/** get table alise*/
	public String getTableAliase() {
		return tableAliase;
	}
	
	/** get table attributes*/
	public LinkedList<String> getAttributes(){
		return attributes;
	}
	
	/** get table name*/
	public String getTableName() {
		return tableName;
	}

	/** get table address*/
	public String getTableAddress() {
		return tableAddress;
	}

	@Override
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
		
	}

	
	
}
