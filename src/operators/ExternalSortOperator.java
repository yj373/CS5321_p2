package operators;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;

import data.Dynamic_properties;
import data.Tuple;
import util.TupleReader;
import util.TupleWriter;
/**
 * The format of temporary scratch file naming: aliase_column_passNum_fileNum_queryNum
 * @author Yixuan Jiang
 *
 */
public class ExternalSortOperator extends Operator{
	
	private int queryNum;
	private File tempDir;
	private File[] scratchFiles; // Current scratch files under the temporary directory 
	private TupleReader tr1;
	private TupleReader tr2;
	private TupleWriter tw;
	private int bufferSize;
	private Tuple[] sortBuffer;
	private static int pageSize = 4096;
	private int tuplePerPage;
	private Tuple[] outputPage;
	private LinkedList<String> attrList; // Records the compar order
	private int passNum = 0;
	
	/**
	 * ExternalSortOperator constructor with configurable buffer size B pages,
	 * B-1 pages are assigned to sort buffer, while 1 page is considered as
	 * the output page
	 * @param queryNumber: query number
	 * @param bSize: buffer size (B)
	 * @param op: left child operator
	 */
	public ExternalSortOperator(int queryNumber, int bSize, LinkedList<String>attributes, Operator op) {
		this.leftChild = op;
		this.queryNum = queryNumber;
		this.bufferSize = bSize;
		int attrNum = this.schema.size();
		this.tuplePerPage = pageSize/(4*attrNum);
		int maxTuple = (bufferSize-1)*tuplePerPage;
		this.sortBuffer = new Tuple[maxTuple];
		this.outputPage = new Tuple[tuplePerPage];
		this.tempDir = new File(Dynamic_properties.tempPath);
		this.scratchFiles = tempDir.listFiles();
		this.attrList = attributes;
		
	}
	
	/**
	 * Read tuples into the sort file if passNum == 0. Clear the sort buffer before reading.
	 * Once the child operator returns no more tuple, set the pass number to 1.
	 */
	private void read() {
		//Clear the sort buffer
		Arrays.fill(this.sortBuffer, null);
		//Read in
		Tuple tuple = this.leftChild.getNextTuple();
		int i = 0;
		while(tuple != null && i < sortBuffer.length) {
			sortBuffer[i] = tuple;
			tuple = this.leftChild.getNextTuple();
		}
		if (tuple == null) {
			this.passNum = 1;
		}
	}
	/**
	 * Read tuples into the sort file if passNum > 0. Clear the sort buffer before reading.
	 * Given two scatch files, construct two tuple readers to read tuples.
	 */
	private void read(File f1, File f2) {
		
	}


	/**
	 * Clean up the temp directory between queries
	 */
	public void cleanTempDirectory() {
		if (scratchFiles.length!=0) {
			String[] temp = scratchFiles[0].getName().split("_");
			int currQueryNum = Integer.valueOf(temp[temp.length-1]);
			if (currQueryNum != queryNum) {
				for (File file: scratchFiles) {
					file.delete();
				}
			}
		}
	}

	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}
	/**
	 * TupleComparator is used to determine the relative positions of two tuples, based on
	 * the comparing order.
	 */	
	class TupleComparator implements Comparator<Tuple>{
		private LinkedList<String> compareOrder;
		public TupleComparator (LinkedList<String> attrList) {
			this.compareOrder = attrList;
		}

		@Override
		public int compare(Tuple o1, Tuple o2) {
			if(compareOrder.size() == 0) return 0;
			for (String collumn: compareOrder) {
				int index = o1.getSchema().get(collumn);
				if (o1.getData()[index]<o2.getData()[index]) {
					return -1;
				}
				if (o1.getData()[index]>o2.getData()[index]) {
					return 1;
				}
			}
			return 0;
		}
		
	}
	
	//Test
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long[] data1 = {1,4,4,4};
		long[] data2 = {1,4,3,3};
		long[] data3 = {6,4,3,1};
		long[] data4 = {2,5,8,5};
		long[] data5 = {2,2,5,5};
		HashMap<String, Integer> schema1 = new HashMap<String, Integer>();
		schema1.put("S.A", 0);
		schema1.put("S.B", 1);
		schema1.put("R.G", 2);
		schema1.put("R.H", 3);
		Tuple t1 = new Tuple(data1, schema1);
		Tuple t2 = new Tuple(data2, schema1);
		Tuple t3 = new Tuple(data3, schema1);
		Tuple t4 = new Tuple(data4, schema1);
		Tuple t5 = new Tuple(data5, schema1);
		Tuple[] arr = {t1,t2,t3,t4,t5};
		LinkedList<String> orderList = new LinkedList<String>();
		orderList.add("R.H");
		orderList.add("S.B");
		//orderList.add("S.A");
		Arrays.sort(arr, new TupleComparator_test(orderList));
		for (Tuple t: arr) {
			t.printData();
		}
		
		
		
		
	
		

	}

}
