package operators;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import data.Dynamic_properties;
import data.Tuple;
import util.TupleReader;
import util.TupleWriter;

public class V2ExternalSortOperator extends Operator{
	private int queryNum;
	private String tempAddress;
	private File tempDir;
	private File[] scratchFiles; // Current scratch files under the temporary directory 
	private TupleReader[] trs;
	//private boolean[] trsStates;
	private TupleWriter tw;
	private int bufferSize;
	private Tuple[] sortBuffer;
	private static int pageSize = 4096;
	private int tuplePerPage;
	//private Tuple[] outputPage;
	private List<String> attrList; // Records the comparing order
	private int passNum = 0;
	private int[] mergePointers; //Records the pointers of merge sort
	//private int outputPointer = 0;
	public V2ExternalSortOperator(int queryNumber, int bSize, List<String>attributes, Map<String, Integer>schema, Operator op) {
		this.leftChild = op;
		this.queryNum = queryNumber;
		this.bufferSize = bSize;
		this.schema = schema;
		int attrNum = this.schema.size();
		this.tuplePerPage = pageSize/(4*attrNum);
		int maxTuple = (bufferSize-1)*tuplePerPage;
		this.sortBuffer = new Tuple[maxTuple];
		this.trs = new TupleReader[bufferSize-1];
		//this.trsStates = new boolean[bufferSize-1];
		//this.outputPage = new Tuple[tuplePerPage];
		StringBuilder sb = new StringBuilder();
		sb.append("exSort-");
		sb.append(op.name);
		sb.append("-");
		sb.append(queryNum);
		name = sb.toString();
		this.tempAddress = Dynamic_properties.tempPath + "/external-sort/" + name;
		this.tempDir = new File(tempAddress);
		if(!tempDir.exists()) {
			tempDir.mkdirs();
		}
		this.scratchFiles = tempDir.listFiles((dir, name) -> !name.equals(".DS_Store"));
		this.attrList = attributes;
		this.mergePointers = new int[bufferSize-1];
		cleanTempDirectory();
		//initMergePointers();
		try {
			readSortWrite();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String fileName = scratchFiles[0].getName().replace("_humanreadable", "");	
		String fileAddress = getFileAddress(fileName);
		trs[0] = new TupleReader(fileAddress, this.schema);
		
	}
	
	/**
	 * Clean up the temp directory between queries
	 */
	public void cleanTempDirectory() {
		File[] scratchDirs = new File(Dynamic_properties.tempPath + "/external-sort").listFiles((dir, name) -> !name.equals(".DS_Store"));
		if (scratchDirs.length!=0) {
			//the first file is a human readable file
			for(File dir : scratchDirs) {
				String[] temp = dir.getName().split("-");
				
				String currQueryNum = temp[temp.length-1];
				if (!currQueryNum.equals(String.valueOf(queryNum)) ) {
					deleteDirectory(dir);
				}
			}
			
		}
		this.scratchFiles = tempDir.listFiles((dir, name) -> !name.equals(".DS_Store"));
	}
	private void deleteDirectory(File dir) {
		File[] contents = dir.listFiles();
		if (contents != null) {
			for (File f: contents) {
				deleteDirectory(f);
			}
		}
		dir.delete();
	}
	
	private void readSortWrite() throws Exception {
		//pass 0
		int readState = 1;
		int fileNum = 0;
		while(readState == 1) {
			readState = readInBuffer();
			Arrays.sort(sortBuffer, new TupleComparator(this.attrList));
			String scratchPath = generatePath(fileNum);
			TupleWriter writer = new TupleWriter(scratchPath);//Needs a null tuple to close
			boolean f = true;
			for(Tuple t: sortBuffer) {
				writer.writeTuple(t);
				if(t == null) {
					f = false;
					break;
				}
			}
			if (f) {
				writer.writeTuple(null);
			}
			fileNum ++;
		}
		this.scratchFiles = tempDir.listFiles((dir, name) -> !name.equals(".DS_Store"));
		Arrays.sort(this.scratchFiles);
		passNum = 1;
		fileNum = 0;
		while (scratchFiles.length > 2) {
			int i = 0;//index of scratch files
			
			int fileCountsBeforePass = scratchFiles.length;
			//Execute a pass
			List<File>deleteFiles = new LinkedList<File>();
			while(i < fileCountsBeforePass) {
				//Construct the tuple readers
				int j = 0;//index of tuple readers
				while (j < trs.length) {
					//if (j >= scratchFiles.length) break;
					String fileName = scratchFiles[i].getName().replace("_humanreadable", "");
					int filePassNum = getFilePassNum(fileName);
					if (filePassNum < passNum) {
						//Delete files of last pass
						String fileAddress1 = getFileAddress(fileName);
						String fileAddress2 = fileAddress1 + "_humanreadable";
						deleteFiles.add(new File(fileAddress1));
						deleteFiles.add(new File(fileAddress2));
						trs[j] = new TupleReader(fileAddress1, this.schema);
						//trsStates[j] = true;
						j++;
					}
					i = i + 2;
					if (i >= fileCountsBeforePass) {
						while (j < trs.length) {
							trs[j] = null;
							j++;
						}
						break;
					}
				}
				
				String scratchPath = generatePath(fileNum);
				tw = new TupleWriter(scratchPath);
				for (int k = 0; k < trs.length; k++) {
					int loadPageI = readInBuffer(k);
					if (loadPageI == 1) {
						mergePointers[k] = k*tuplePerPage;
					}else {
						mergePointers[k] = -1;
					} 
				}
				Tuple tuple = mergeSort();
				int count = 0;
				while(tuple != null) {
					tw.writeTuple(tuple);
					count ++;
					tuple = mergeSort();
				}
				tw.writeTuple(null);
				System.out.println(count);
				fileNum++;
				//this.scratchFiles = tempDir.listFiles((dir, name) -> !name.equals(".DS_Store"));
			}
			//Delete all the files of this last pass
			for (File f: deleteFiles) {
				f.delete();
			}
			passNum++;
			this.scratchFiles = tempDir.listFiles((dir, name) -> !name.equals(".DS_Store"));
			Arrays.sort(this.scratchFiles);
			fileNum = 0;
		}
	}
	
	private int readInBuffer() throws Exception {
		//Clear the sort buffer
		Arrays.fill(this.sortBuffer, null);
		//Read in
		Tuple tuple = this.leftChild.getNextTuple();
		int i = 0;
		while(tuple != null && i < sortBuffer.length-1) {
			sortBuffer[i] = tuple;
			tuple = this.leftChild.getNextTuple();
			i++;
		}
		//initMergePointers();
		if (tuple == null) {
			return 0;
		}else {
			sortBuffer[i] = tuple;
			return 1;
		}
	}
	
	private int readInBuffer(int i) throws Exception {
		//Clear ith page
		for (int k = i*tuplePerPage; k < (i+1)*tuplePerPage; k++) {
			sortBuffer[k] = null;
		}
		if (i < trs.length && trs[i] != null) {
			int tupNum = 0;
			Tuple tuple = trs[i].readNextTuple();
			while(tuple != null && tupNum < tuplePerPage-1) {
				int index = i*tuplePerPage+tupNum;
				sortBuffer[index] = tuple;
				tuple = trs[i].readNextTuple();
				tupNum++;
			}
			if (tupNum == 0) return 0;
			else {
				sortBuffer[(i+1)*tuplePerPage-1] = tuple;
				return 1;
			}
		}
		return 0;
	}
	
	private Tuple mergeSort() throws Exception {
		TupleComparator tc = new TupleComparator(this.attrList);
		int currInd = -1;
		for (int i = 0; i < mergePointers.length; i++) {
			if (mergePointers[i] != -1) {
				currInd = i;
				break;
			}
		}
		if(currInd != -1 ) {
			Tuple res = sortBuffer[mergePointers[currInd]];
			boolean flag = true;
			while(flag) {
				flag = false;
				int j = currInd + 1;
				while (j < mergePointers.length) {
					if(mergePointers[j] == -1) {
						j++;
						continue;
					}
					Tuple cand = sortBuffer[mergePointers[j]];
					if (tc.compare(cand, res)==-1) {
						flag = true;
						res = cand;
						currInd = j;
						j = currInd + 1;
					}
					j++;
				}
			}
			mergePointers[currInd]++;
			if (mergePointers[currInd]==(currInd+1)*tuplePerPage) {
				readInBuffer(currInd);
				mergePointers[currInd] = currInd*tuplePerPage;
				if (sortBuffer[mergePointers[currInd]]==null) {
					mergePointers[currInd] = -1;
				}
			}else if (sortBuffer[mergePointers[currInd]]==null) {
				mergePointers[currInd] = -1;
			}
			return res;
			
		}else {
			return null;
		}
	}
	
	private String generatePath(int fileNum) {
		StringBuilder sb = new StringBuilder();
		sb.append(tempAddress);
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
		sb.append(tempAddress);
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
	
	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		try {
			return this.trs[0].readNextTuple();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void reset() {
		if (trs[0] != null) {
			this.trs[0].reset();
		}
	}
	
	@Override
	public void reset(int idx) {
		this.trs[0].resetFileChannel(idx);
	}
	
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

}
