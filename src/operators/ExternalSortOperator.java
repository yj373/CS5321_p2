package operators;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
	private TupleReader[] trs;
	private TupleWriter tw;
	private int bufferSize;
	private Tuple[] sortBuffer;
	private static int pageSize = 4096;
	private int tuplePerPage;
	private Tuple[] outputPage;
	private List<String> attrList; // Records the comparing order
	private int passNum = 0;
	private int[] mergePointers; //Records the pointers of merge sort
	private int outputPointer = 0;
	
	/**
	 * ExternalSortOperator constructor with configurable buffer size B pages,
	 * B-1 pages are assigned to sort buffer, while 1 page is considered as
	 * the output page
	 * @param queryNumber: query number
	 * @param bSize: buffer size (B)
	 * @param op: left child operator
	 */
	public ExternalSortOperator(int queryNumber, int bSize, List<String>attributes, Map<String, Integer>schema, Operator op) {
		this.leftChild = op;
		this.queryNum = queryNumber;
		this.bufferSize = bSize;
		int attrNum = this.schema.size();
		this.tuplePerPage = pageSize/(4*attrNum);
		int maxTuple = (bufferSize-1)*tuplePerPage;
		this.sortBuffer = new Tuple[maxTuple];
		this.trs = new TupleReader[bufferSize-1];
		this.outputPage = new Tuple[tuplePerPage];
		this.tempDir = new File(Dynamic_properties.tempPath);
		this.scratchFiles = tempDir.listFiles();
		this.attrList = attributes;
		this.mergePointers = new int[bufferSize-1];
		this.schema = schema;
		//initMergePointers();
		
	}
	private void initMergePointers() {
		for (int i = 0; i < mergePointers.length; i++) {
			mergePointers[i] = i*tuplePerPage;
			if(sortBuffer[mergePointers[i]]==null) {
				mergePointers[i] = -1;
			}
		}
	}
	
	/**
	 * Read tuples into the sort buffer by getting tuples from the child operator
	 * or from the tuple readers stored in the operator. Clear the sort buffer 
	 * before reading. Once the child operator returns no more tuple, set the pass number to 1.
	 * @throws Exception
	 * @return 0: phase0 is ended
	 * 		   1: phase0 is not ended but sort buffer is full
	 * 		   2: all the tuple readers have finished reading 
	 * 		   3: some tuple readers have not finished reading. Each tuple reader loads at most
	 * 			  one page to in the sort buffer
	 */
	private int readInBuffer() throws Exception {
		//Clear the sort buffer
		Arrays.fill(this.sortBuffer, null);
		//Read in
		if (trs[0] == null) {
			Tuple tuple = this.leftChild.getNextTuple();
			int i = 0;
			while(tuple != null && i < sortBuffer.length) {
				sortBuffer[i] = tuple;
				tuple = this.leftChild.getNextTuple();
			}
			//initMergePointers();
			if (tuple == null) {
				return 0;
			}else {
				return 1;
			}
		}else {
			int tCount = 0;
			for (int i = 0; i < trs.length; i++) {
				int tupNum = 0;
				Tuple tuple = trs[i].readNextTuple();
				while(tuple != null && tupNum < tuplePerPage) {
					int index = i*tuplePerPage+tupNum;
					sortBuffer[index] = tuple;
					tuple = trs[i].readNextTuple();
					tupNum++;
					tCount++;
				}
			}
			initMergePointers();
			if (tCount == 0) return 2;
			else return 3;
		}
		
	}
	
	/**
	 * Read tuples into the sort buffer if passNum > 0. Clear the sort buffer before reading.
	 * Refresh the tuple readers stored in the operator, and read by the new tuple readers.
	 * @throws Exception 
	 * @return 0: all the tuple readers have finished reading
	 * 		   1: some tuple readers have not finished reading. Each tuple reader loads at most
	 * 			  one page to in the sort buffer
	 */
//	private int readInBuffer(TupleReader[] tupleReaders) throws Exception {
//		//Clear the sort buffer
//		Arrays.fill(this.sortBuffer, null);
//		//Read in
//		for(int i = 0; i < Math.min(tupleReaders.length, trs.length); i++) {
//			trs[i] = tupleReaders[i];
//		}
//		int tCount = 0;
//		for (int i = 0; i < trs.length; i++) {
//			int tupNum = 0;
//			Tuple tuple = trs[i].readNextTuple();
//			while(tuple != null && tupNum < tuplePerPage) {
//				int index = i*tuplePerPage+tupNum;
//				sortBuffer[index] = tuple;
//				tuple = trs[i].readNextTuple();
//				tupNum++;
//				tCount++;
//			}
//		}	
//		initMergePointers();
//		if (tCount == 0) return 0;
//		else return 1;
//	}
	
	/**
	 * Write output page to scratch file, using the tuple writer stored in the class.
	 * @param flag
	 * @throws IOException
	 * @return 0: output page is not full
	 * 		   1: output page is full
	 * 		   -1: tw is null
	 */
	private void writeToScratch() throws IOException {
		int i=0;
		if (tw!=null) {
			while(i < outputPage.length && outputPage[i] != null) {
				tw.writeTuple(outputPage[i]);
				i++;
			}
			
			Arrays.fill(outputPage, null);
			outputPointer = 0;
			
		}
	}
	
	/**
	 * Write the output page to scratch file. Refresh the tuple writer, and write by the new
	 * tuple writer.
	 * @param tupleWriter: tuple writer of a new scratch file
	 * @throws IOException
	 * @return 0: output page is not full
	 * 		   1: output page is full 
	 */
//	private int writeToScratch(TupleWriter tupleWriter) throws IOException {
////		StringBuilder sb = new StringBuilder();
////		sb.append(Dynamic_properties.tempPath);
////		sb.append("/fileName");
////		tw = new TupleWriter(sb.toString());
//		tw = tupleWriter;
//		int i = 0;
//		while(i < outputPage.length && outputPage[i] != null) {
//			tw.writeTuple(outputPage[i]);
//			i++;
//		}
//		if (outputPage[i] == null) return 0;
//		else return 1;	
		
		
	//}
	/**
	 * One iteration of merge sort. During the iteration find the smallest tuple pointed by the merge pointers.
	 * After finding the smallest tuple, add the corresponding merge pointer by 1. If the merge pointer is 
	 * larger than the largest possible value, the merge pointer will be set to -1. The output pointer will also
	 * be added by 1.
	 * @return 0: the merge is not done 
	 * 		   1: the merge is done, all merge pointers == -1
	 * 		   2: the merge is done, the output page is full
	 */
	public int mergeSort() {
		TupleComparator tc = new TupleComparator(this.attrList);
		int currInd = -1;
		for (int i = 0; i < mergePointers.length; i++) {
			if (mergePointers[i] != -1) {
				currInd = i;
				break;
			}
		}
		if(currInd != -1 && outputPointer < outputPage.length) {
			Tuple res = sortBuffer[mergePointers[currInd]];
			boolean flag = true;
			while(flag) {
				flag = false;
				for (int j = currInd + 1; j < mergePointers.length; j++) {
					if(mergePointers[j] == -1) continue;
					Tuple cand = sortBuffer[mergePointers[j]];
//					if (cand == null) {
//						mergePointers[j] = -1;
//						continue;
//					} 
					if (tc.compare(cand, res)==-1) {
						flag = true;
						res = cand;
						currInd = j;
					}
				}
			}
			outputPage[outputPointer] = res;
			outputPointer++;
			mergePointers[currInd]++;
			if(mergePointers[currInd]==(currInd+1)*tuplePerPage || sortBuffer[mergePointers[currInd]]==null) {
				mergePointers[currInd] = -1;
			}
			return 0;
			
		}else {
			if(currInd == -1) return 1;
			else return 2;
		}
		
	}
	
	private String generatePath(int fileNum) {
		StringBuilder sb = new StringBuilder();
		sb.append(Dynamic_properties.tempPath);
		sb.append('/');
		for (String s: this.attrList) {
			s = s.replace('.', '_');
			sb.append(s);
			sb.append('_');
		}
		sb.append(passNum);
		sb.append("_");
		sb.append(fileNum);
		sb.append("_");
		sb.append(queryNum);
		String scratchPath = sb.toString();
		return scratchPath;
	}
	private String getFileAddress(String fileName) {
		StringBuilder sb = new StringBuilder();
		sb.append(Dynamic_properties.tempPath);
		sb.append('/');
		sb.append(fileName);
		return sb.toString();
	}
	private int getFilePassNum(String fileName) {
		String[] s = fileName.split("_");
		if (s[s.length-1].equals("humanreadable")) {
			return Integer.valueOf(s[s.length-4]);
		}else return Integer.valueOf(s[s.length-3]);
		
	}
	/**
	 * Doing the sort with the help of temporary scratch files. Doing in memory sort in the pass 0.
	 * Doing the B-1 way external sort in the later passes
	 * @throws Exception 
	 */
	private void sort() throws Exception {
		//pass 0
		int readState = 1;
		int fileNum = 0;
		while(readState == 1) {
			readState = readInBuffer();
			Arrays.sort(sortBuffer, new TupleComparator(this.attrList));
			String scratchPath = generatePath(fileNum);
			TupleWriter writer = new TupleWriter(scratchPath);
			for(Tuple t: sortBuffer) {
				writer.writeTuple(t);
			}
			fileNum ++;
		}
		this.scratchFiles = tempDir.listFiles();
		passNum = 1;
		fileNum = 0;
		while (scratchFiles.length > 2) {
			int i = 0;//index of scratch files
			int j = 0;//index of tuple readers
			//Execute a pass
			while(i < scratchFiles.length ) {
				//Construct the tuple readers
				List<File>deleteFiles = new LinkedList<File>();
				while (j < trs.length) {
					//if (j >= scratchFiles.length) break;
					String fileName = scratchFiles[i].getName();
					String[] s  = fileName.split("_");
					if(s[s.length-1].equals("humanreadable")) {
						//skip human readable files
						i++;
						continue;
					}
					int filePassNum = getFilePassNum(fileName);
					if (filePassNum < passNum) {
						//Delete files of last pass
						String fileAddress1 = getFileAddress(fileName);
						String fileAddress2 = fileAddress1 + "_humanreadable";
						deleteFiles.add(new File(fileAddress1));
						deleteFiles.add(new File(fileAddress2));
						trs[j] = new TupleReader(fileAddress1, this.schema);
						j++;
					}
					i++;
					if (i >= scratchFiles.length) break;
				}
				//Execute a run in this pass
				readState = 3;//reading is not finished
				int mergeState = 1;//merging is not finished, needs to load new data
				
				String scratchPath = generatePath(fileNum);
				tw = new TupleWriter(scratchPath);
				while(readState == 3) {
					if (mergeState == 1) {
						readState = readInBuffer();
						mergeState = 0;
					}
					while(mergeState == 0) {
						mergeState = mergeSort();
						if (mergeState == 2) {
							writeToScratch();
							mergeState = 0;
						} 
						
					}
				}
				fileNum++;
				//Delete all the files of this last pass
				for (File f: deleteFiles) {
					f.delete();
				}
			}
			passNum++;
			this.scratchFiles = tempDir.listFiles();
		}	
		
	}


	/**
	 * Clean up the temp directory between queries
	 */
	public void cleanTempDirectory() {
		if (scratchFiles.length!=0) {
			//the first file is a human readable file
			String[] temp = scratchFiles[0].getName().split("_");
			int currQueryNum = 0;
			if (temp[temp.length-1].equals("humanhumanreadable")) {
				currQueryNum = Integer.valueOf(temp[temp.length-2]);
			}else {
				currQueryNum = Integer.valueOf(temp[temp.length-1]);
			}
			if (currQueryNum != queryNum) {
				for (File file: scratchFiles) {
					file.delete();
				}
			}
		}
	}

	@Override
	public Tuple getNextTuple() {
		try {
			sort();
			if(this.scratchFiles.length==2) {
				String fileName = scratchFiles[1].getName();
				String fileAddress = getFileAddress(fileName);
				trs[0] = new TupleReader(fileAddress, this.schema);
				return trs[0].readNextTuple();
			}else {
				System.out.println("the number of scratch files is wrong!");
				return null;
			}
		}catch(Exception e) {
			System.out.println("Exception occurs in external sort");
			e.getMessage();
			e.printStackTrace();
		}
		
		return null;
	}

	@Override
	public void reset() {
		this.trs[0].reset();
		
	}
	/**
	 * TupleComparator is used to determine the relative positions of two tuples, based on
	 * the comparing order.
	 */	
	class TupleComparator implements Comparator<Tuple>{
		private List<String> compareOrder;
		public TupleComparator (List<String> attrList) {
			this.compareOrder = attrList;
		}

		@Override
		public int compare(Tuple o1, Tuple o2) {
			if(compareOrder.size() == 0) return 0;
			if (o1 == null && o2 == null) {
				return 0;
			}
			if (o1 == null) {
				return 1;
			}
			if (o2 == null) {
				return -1;
			}
			for (String collumn: compareOrder) {
				int index = o1.getSchema().get(collumn);
				if (o1.getData()[index]<o2.getData()[index]) {
					return -1;
				}
				if (o1.getData()[index]>o2.getData()[index]) {
					return 1;
				}
			}
			for (int i = 0; i < o1.getSchema().size(); i++) {
				if (o1.getData()[i] < o2.getData()[i]) {
					return -1;
				} 
				if (o1.getData()[i] > o2.getData()[i]){
					return 1;
				} 
			}
			return 0;
		}
		
	}
	
	//Test
//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//		long[] data1 = {1,4,4,4};
//		long[] data2 = {1,4,3,3};
//		long[] data3 = {6,4,3,1};
//		long[] data4 = {2,5,5,5};
//		long[] data5 = {2,2,5,5};
//		HashMap<String, Integer> schema1 = new HashMap<String, Integer>();
//		schema1.put("S.A", 0);
//		schema1.put("S.B", 1);
//		schema1.put("R.G", 2);
//		schema1.put("R.H", 3);
//		Tuple t1 = new Tuple(data1, schema1);
//		Tuple t2 = new Tuple(data2, schema1);
//		Tuple t3 = new Tuple(data3, schema1);
//		Tuple t4 = new Tuple(data4, schema1);
//		Tuple t5 = new Tuple(data5, schema1);
//		Tuple[] arr = {t1,t2,t3,t4,t5, null};
//		LinkedList<String> orderList = new LinkedList<String>();
//		orderList.add("R.H");
//		orderList.add("S.B");
//		//orderList.add("S.A");
//		Arrays.sort(arr, new TupleComparator_test(orderList));
//		for (Tuple t: arr) {
//			if (t!=null) {
//				t.printData();
//			}
//			
//		}
//
//	}

}
