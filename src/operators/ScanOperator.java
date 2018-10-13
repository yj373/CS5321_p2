package operators;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import data.DataBase;

import data.Tuple;

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
	private RandomAccessFile readPointer;
	private String tableAliase;
	private LinkedList<String> attributes;
	

	/**
	 * This method is to get the next tuple after scanning
	 * 
	 * @return next tuple after scanning
	 */
	@Override
	public Tuple getNextTuple() {
		try {
			String data = readPointer.readLine();
			if (data!=null) {
				/*Handle aliases*/
				Tuple tuple = new Tuple(data, tableAliase, attributes);
				return tuple;
			}
		} catch (IOException e) {
			e.printStackTrace();
			e.getMessage();
		}
		
		return null;
	}

	/**
	 * This method is to reset scan operator
	 * by resetting its readpointer
	 */
	@Override
	public void reset() {
		try {
			this.readPointer.seek(0);
		} catch (IOException e) {
			e.printStackTrace();
			e.getMessage();
		}
		
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
	public ScanOperator(String tableInfo) {
		String[] aimTable = tableInfo.split("\\s+");
		if (aimTable.length<1) {
			this.tableName = null;
			return;
		}
		this.tableName = aimTable[0];
		this.tableAddress = DataBase.getInstance().getAddresses(tableName);
		this.tableFile = new File(tableAddress);
		try {
			this.readPointer = new RandomAccessFile(this.tableFile, "r");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			e.getMessage();
		}
		this.tableAliase = aimTable[aimTable.length-1];
		this.attributes = DataBase.getInstance().getSchema(tableName);
	}
	
	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 * 
	 * @param tableName
	 * @param tableAliase
	 * 
	 */
	public ScanOperator(String tableName, String tableAliase) {
		this.tableName = tableName;
		this.tableAddress = DataBase.getInstance().getAddresses(tableName);
		this.tableFile = new File(tableAddress);
		try {
			this.readPointer = new RandomAccessFile(this.tableFile, "r");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			e.getMessage();
		}
		if (tableAliase == null) this.tableAliase = tableName;
		else this.tableAliase = tableAliase;
		this.attributes = DataBase.getInstance().getSchema(tableName);
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
		setParent(op);
		this.tableName = tableName;
		this.tableAddress = DataBase.getInstance().getAddresses(tableName);
		this.tableFile = new File(tableAddress);
		try {
			this.readPointer = new RandomAccessFile(this.tableFile, "r");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			e.getMessage();
		}
		if (tableAliase == null) this.tableAliase = tableName;
		else this.tableAliase = tableAliase;
		this.attributes = DataBase.getInstance().getSchema(tableName);
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

	/** get table file*/
	public File getTableFile() {
		return tableFile;
	}

	/** get table read pointer*/
	public RandomAccessFile getReadPointer() {
		return readPointer;
	}

}