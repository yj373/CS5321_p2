package operators;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;

import data.Dynamic_properties;
import data.Tuple;
import util.TupleReader;
import util.TupleWriter;
/**
 * The format of temporary scratch file naming: aliase_column_passNum_queryNum
 * @author Yixuan Jiang
 *
 */
public class ExternalSortOperator extends Operator{
	
	private int queryNum;
	private String tempDirPath;
	private TupleReader tr1;
	private TupleReader tr2;
	private TupleWriter tw;
	private int bufferSize;
	private Tuple[] sortBuffer;
	private static int pageSize = 4096;
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
		int tuplePerPage = pageSize/(4*attrNum);
		int maxTuple = (bufferSize-1)*tuplePerPage;
		this.sortBuffer = new Tuple[maxTuple];
		this.outputPage = new Tuple[tuplePerPage];
		this.tempDirPath = Dynamic_properties.tempPath;
		this.attrList = attributes;
		
	}
	
	/**
	 * Clear the sort buffer. Read as many tuples as possible into the sort buffer.
	 * Sort all the tuples in the sort buffer in memory.
	 */
	private void readAndSort() {
		//Clear the sort buffer
		Arrays.fill(this.sortBuffer, null);
		//Read in
		if (passNum == 0) {
			Tuple tuple = this.leftChild.getNextTuple();
			int i = 0;
			while(tuple != null && i < sortBuffer.length) {
				sortBuffer[i] = tuple;
				tuple = this.leftChild.getNextTuple();
			}
		}
		
		Arrays.sort(sortBuffer, new TupleComparator(this.attrList));
	}


	/**
	 * Clean up the temp directory between queries
	 */
	public void cleanTempDirectory() {
		File folder = new File(tempDirPath);
		File[] fileList = folder.listFiles();
		if (fileList.length!=0) {
			String[] temp = fileList[0].getName().split("_");
			int currQueryNum = Integer.valueOf(temp[temp.length-1]);
			if (currQueryNum != queryNum) {
				for (File file: fileList) {
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

	}

}
