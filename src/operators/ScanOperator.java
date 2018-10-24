package operators;

import java.io.File;
import java.util.LinkedList;
import data.DataBase;

import data.Tuple;
import net.sf.jsqlparser.expression.Expression;
import util.TupleReader;

/**
 * this class provides function:
 * scan all tuples in a table 
 * 
 * @author Yixuan Jiang
 *
 */

public class ScanOperator extends Operator{
	
	private String tableName;
	private String tableAddress;
	private File tableFile;
	//private RandomAccessFile readPointer;
	private String tableAliase;
	private LinkedList<String> attributes;
	private TupleReader tr;
	

	/**
	 * This method is to get the next tuple after scanning
	 * 
	 * @return next tuple after scanning
	 */
	@Override
	public Tuple getNextTuple() {
		try {
			//String data = tr.readNextTuple().getTupleData();
			Tuple t = tr.readNextTuple();
			if (t!=null) {
				/*Handle aliases*/
				//Tuple t = new Tuple(data, tableAliase, attributes);
				Expression e = this.getExpression();
				if(e!=null) {
					while (t!=null) {
						boolean res = super.judgeExpression(t);
						if(res) break;
						t = tr.readNextTuple();				
					}			
				}
				return t;
			}
		} catch (Exception e) {
			e.printStackTrace();
			e.getMessage();
		}
		
		return null;
	}

	/**
	 * This method is to reset scan operator
	 * by resetting its tuple reader
	 */
	@Override
	public void reset() {
		try {
			this.tr.reset();
		} catch (Exception e) {
			e.printStackTrace();
			e.getMessage();
		}
		
	}
	
	public void reset(int bufferIndex, int fileChannelIndex) {
		this.tr.resetBuffer(bufferIndex);
		this.tr.resetFileChannel(fileChannelIndex);
	}
	
	/**
	 * default constuctor
	 */
	public ScanOperator() {
		
	}
	
	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 * 
	 * @param tableInfo table information
	 * 
	 */
	public ScanOperator(String tableInfo, Expression expression) {
		String[] aimTable = tableInfo.split("\\s+");
		if (aimTable.length<1) {
			this.tableName = null;
			return;
		}
		this.tableName = aimTable[0];
		this.tableAddress = DataBase.getInstance().getAddresses(tableName);
		this.tableFile = new File(tableAddress);
		this.tr = new TupleReader(tableInfo);
		this.tableAliase = aimTable[aimTable.length-1];
		this.attributes = DataBase.getInstance().getSchema(tableName);
		setExpression(expression);
		
		//modification
		schema = tr.getSchema();
	}
	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 * 
	 * @param tableInfo table information
	 * 
	 */
	public ScanOperator(String tableInfo) {
		String[] aimTable = tableInfo.split("\\s+");
		if (aimTable.length<1) {
			this.tableName = null;
			return;
		}
		this.tableName = aimTable[0];
		this.tableAddress = DataBase.getInstance().getAddresses(tableName);
		this.tableFile = new File(tableAddress);
		this.tr = new TupleReader(tableInfo);
		this.tableAliase = aimTable[aimTable.length-1];
		this.attributes = DataBase.getInstance().getSchema(tableName);
		//setExpression(expression);
		
		//modification
		schema = tr.getSchema();
	}
	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 * 
	 * @param tableName
	 * @param tableAliase
	 * 
	 */
	public ScanOperator(String tableName, String tableAliase, Expression expression) {
		this.tableName = tableName;
		this.tableAddress = DataBase.getInstance().getAddresses(tableName);
		this.tableFile = new File(tableAddress);
		StringBuilder sb = new StringBuilder();
		sb.append(tableName);
		sb.append(' ');
		sb.append(tableAliase);
		this.tr = new TupleReader(sb.toString());
		if (tableAliase == null) this.tableAliase = tableName;
		else this.tableAliase = tableAliase;
		this.attributes = DataBase.getInstance().getSchema(tableName);
		setExpression(expression);
		
		//modification
		schema = tr.getSchema();
	}
	
	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 * 
	 * @param tableName
	 * @param tableAliase
	 * @param operator
	 * 
	 */
	public ScanOperator(String tableName, String tableAliase, Operator op) {
		this.tableName = tableName;
		this.tableAddress = DataBase.getInstance().getAddresses(tableName);
		this.tableFile = new File(tableAddress);
		StringBuilder sb = new StringBuilder();
		sb.append(tableName);
		sb.append(' ');
		sb.append(tableAliase);
		this.tr = new TupleReader(sb.toString());
		if (tableAliase == null) this.tableAliase = tableName;
		else this.tableAliase = tableAliase;
		this.attributes = DataBase.getInstance().getSchema(tableName);
		
		
		//modification
		schema = tr.getSchema();
	}
	
	/** get table aliase*/
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

	/** get table file*/
	public File getTableFile() {
		return tableFile;
	}

	/** get table read pointer*/
	public TupleReader getReadPointer() {
		return tr;
	}

}