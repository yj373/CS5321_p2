package data;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * This class provides function:
 * construct Tuple class to store information of every tuple
 * 
 * @author Yixuan Jiang
 *
 */

public class Tuple {
	/*Store the data of each tuple*/
	private long[] data;
	private Map<String, Integer> schema;
	
	/**
	 * default constructor
	 */
	public Tuple() {
		
	}
	
	/** 
	 * This method is a constructor which is to
	 * construct a tuple of a single table
	 * 
	 */
	public Tuple(String str, String table_aliase, LinkedList<String> attributes) {
		String[] temp = str.split(",");
		data = new long[temp.length];
		if (data.length!=attributes.size()) return;
		schema = new HashMap<String, Integer>();
		for (int i=0; i<temp.length; i++) {
			data[i] = Long.parseLong(temp[i]);
			StringBuilder sb = new StringBuilder();
			sb.append(table_aliase);
			sb.append(".");
			sb.append(attributes.get(i));
			schema.put(sb.toString(), i);
		}
	}
	
	
	/** 
	 * This method is a constructor which is to
	 * construct a tuple from previous tuple
	 * 
	 */
	public Tuple(long[] data, Map<String, Integer> schema) {
		this.data = data;
		this.schema = schema;
	
	}
	
	public Tuple(String[] str_arr) {
		data = new long[str_arr.length];
		for (int i=0; i<data.length; i++) {
			data[i] = Long.parseLong(str_arr[i]);
		}
	}
	
	/**
	 * this method is to get data from Tuple
	 */
	public String getTupleData() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i<data.length; i++) {
			sb.append(String.valueOf(data[i]));
			sb.append(",");
		}
		sb.deleteCharAt(sb.length()-1);
		return sb.toString();
	}
	
	
	/**
	 * get data
	 * @return array
	 */
	public long[] getData() {
		return data;
	}
	
	/**
	 * get schema
	 */
	public Map<String, Integer> getSchema() {
		return schema;
	}
	
	/**
	 * get size
	 */
	public int getSize() {
		if (data == null) {
			return 0;
		}else {
			return data.length;
		}
	}
	
	/**
	 * print data in Tuple
	 */
	
	public void printData() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i<data.length; i++) {
			sb.append(String.valueOf(data[i]));
			sb.append(",");
		}
		sb.deleteCharAt(sb.length()-1);
		System.out.println(sb.toString());
		
	}
	

}
