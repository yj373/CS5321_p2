package util;

/**
 * This class provides function:
 * 
 * read binary input file 
 * 
 * @author Xiaoxing Yan
 *
 */

import java.io.File;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;  
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import data.DataBase;
import data.Tuple;

public class TupleReader {

	/*the next position we want to read from the buffer*/
	private int bufferPosition;
	/*the next position we want to read from the channel*/
	private long filePosition;
	/*check whether the buffer is empty or not*/
	private boolean empty;
	/*buffer store bytes in one page*/
	private ByteBuffer buffer;
	/*store the number of attributes in one tuple*/
	private int attributeNumber;
	/*the number of tuples we can read in the buffer or page*/
	private int tupleNumber;
	/*the file channel we are using*/
	private FileChannel fcin;
	/*the ith page we are reading in this file*/
	private int pageNumber;
	/*the size of buffer*/
	private static int size = 4096;

	/*for constructing tuple in this table*/
	private String tableInfo;
	private String tableName;
	private String tableAddress;
	private File tableFile;
	private RandomAccessFile readPointer;
	private String tableAliase;
	private LinkedList<String> attributes;
	
	//modification -- add new field
	private Map<String, Integer> schema;
	//private boolean schemaIsInit; 之后再优化

	
	public Map<String, Integer> getSchema() {
		return schema;
	}

	/** 
	 * This method is a constructor which is to
	 * init file path and related field
	 * 
	 * @param tableInfo decide which table we want to read
	 * 
	 */
	public TupleReader (String tableInfo) {
		try {
			empty = true;
			filePosition = 0;
			bufferPosition = 0;
			pageNumber = 0;
			this.tableInfo = tableInfo;
			/*create a new buffer with size 4096 bytes*/ 
			buffer = ByteBuffer.allocate(size); 
			/*open file channel*/
			initFileChannel (tableInfo);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// Constructor for in-memory sort and external sort;
	public TupleReader (String tableAddress, Map<String, Integer> schema) {
		empty = true;
		filePosition = 0;
		bufferPosition = 0;
		pageNumber = 0;
		this.tableAddress = tableAddress;
		this.schema = schema;
		this.attributeNumber = schema.size();
		/*create a new buffer with size 4096 bytes*/ 
		buffer = ByteBuffer.allocate(size);

		/*open file channel*/
		File tableFile = new File(tableAddress);
		try {
			/*get the channel of source file*/ 
			fcin = new RandomAccessFile(tableFile, "r").getChannel();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			e.getMessage();
		}		
	}



	/**
	 * This method is to init file channel
	 * 
	 * @param tableInfo
	 * @throws Exception
	 */
	private void initFileChannel (String tableInfo) throws Exception {


		String[] aimTable = tableInfo.split("\\s+");
		if (aimTable.length<1) {
			tableName = null;
		}

		/*get the information of table*/
		tableName = aimTable[0];
		tableAddress = DataBase.getInstance().getAddresses(tableName);
		tableAliase = aimTable[aimTable.length-1];
		attributes = DataBase.getInstance().getSchema(tableName);
		File tableFile = new File(tableAddress);

		/*10.22 modification --- init schema by database*/
		schema = new HashMap<String, Integer>();
		for (int i=0; i< attributes.size(); i++) {
			StringBuilder sb = new StringBuilder();
			sb.append(tableAliase);
			sb.append(".");
			sb.append(attributes.get(i));
			schema.put(sb.toString(), i);
		}

		
		
		try {
			/*get the channel of source file*/ 
			fcin = new RandomAccessFile(tableFile, "r").getChannel();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			e.getMessage();
		}		

	}

	
	//main method to test
//	public static void main(String[] args) throws Exception {
//		TupleReader test = new TupleReader("Boats AS B");
//		for (int i=0; i< 1001; i++) {
//			
//			System.out.println(test.readNextTuple().getTupleData());
//		}
//		//test.readNextTuple();
//		
//	}
	


	/**
	 * read the next tuple from buffer 
	 * 
	 * @return tuple
	 * @throws Exception
	 */
	public Tuple readNextTuple() throws Exception {

		/*if buffer is empty, it needs to load another page*/
		if (empty) {
			int r = readFromChannel();
			/*if we have reached the end of file*/
			if (r == -1) {
				close();
				reset();
				return null;  
			} 
			empty = false;
		} 

		/*initilize metadata if this buffer read a new page*/
		checkBuffer();

		/*read one integer every time from the buffer*/
		long[] data = new long[attributeNumber];
		for (int i=0; i<attributeNumber; i++) {
			data[i] = buffer.getInt(bufferPosition);
			bufferPosition += 4;
		}
	
		/*create a new tuple*/
		Tuple tuple = new Tuple(data, schema);
		
		tupleNumber--;

		/* after reading all tuples in one page
		 * load another page to buffer
		 **/
		if (tupleNumber == 0) {
			empty = true;
		}

		return tuple;
	}

	/**
	 * this method is to check the buffer state:
	 * 
	 * to read metadata
	 * or to read tuple
	 * 
	 */
	public void checkBuffer() {
		if (bufferPosition < 8) {
			/*initilize metadata*/
			attributeNumber = buffer.getInt(bufferPosition);
			bufferPosition += 4;
			tupleNumber = buffer.getInt(bufferPosition);
			bufferPosition += 4;	
		}
	}


	/**
	 * this method id to load one page to buffer 
	 * 
	 * 
	 * @return r
	 * @throws Exception
	 */
	private int readFromChannel() throws IOException  {
		if (!fcin.isOpen()) {
			File tableFile = new File(tableAddress);
			fcin = new RandomAccessFile(tableFile, "r").getChannel();
		}
		pageNumber+=1;
		//System.out.println("第"+pageNumber+"页");

		/* clear buffer to accept new data*/
		buffer.clear();  
		/*set the buffer position to zero*/
		bufferPosition = 0;
		
		//System.out.println("读之前这里的file position是"+filePosition);

		fcin.position(filePosition);
		/*Reads a sequence of bytes from this channel into the given buffer.*/
		int r = fcin.read(buffer); 
		/*record the next position*/
		filePosition = fcin.position();
		
		//System.out.println("读之后这里的file position是"+filePosition);

		
		//buffer.flip();
	
		return r;

	}



	/**
	 * this method is to close the file channel
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {

		try {
			if(fcin != null) {
				fcin.close();
			}

		} catch(IOException e) {
			throw e;
		}

	}

	
	/**
	 * 
	 */
	public void reset() {
		this.bufferPosition = 0;
		this.filePosition = 0;
		this.empty = true;
	}
	
	public void resetBuffer(int index) {
		this.bufferPosition = index;
	}
	
	/*
	 * param index: the NO.i tuple that I want to get in the File table;
	 */
	public void resetFileChannel(int index) {
		
		/* supposing the file is n pages; for the first n - 1 pages, 
		 the number of tuples on each page is:
		     numFirst = (4096 - 8) / (4 * attribute_num);
		 the number of tuples on the last page is:
		     numLast = index % numFirst 
		 */
		int numFirst = (TupleReader.size - 8) / (4 * this.schema.size());
		int numLast = index % numFirst;
		pageNumber = index / numFirst;
		filePosition = TupleReader.size * pageNumber;
		try {
			int res = readFromChannel();  // In this func, we load a new buffer page so pageNumber++
			                              // through channel, start with the specified file position
			if (res == -1) {
				close();
				reset();
			} 
			empty = false;
		} catch (IOException e) {
			e.printStackTrace();
		}
		resetBuffer(numLast * (4 * this.schema.size()));
	}

}



