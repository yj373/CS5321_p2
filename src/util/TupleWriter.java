package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import util.TupleReader;
import data.Dynamic_properties;
import data.Tuple;

public class TupleWriter {
	

	/*the next position we want to read from the buffer*/
	private int bufferPosition;
	/*check whether the buffer is empty or not*/
	private boolean empty;
	/*buffer store bytes in one page*/
	private ByteBuffer buffer;
	/*store the number of attributes in one tuple*/
	private int attributeNumber;
	/*maximum number of tuples we can write in one page*/
	private int maxTupleNumber;
	/*the number of tuples we have written in the buffer*/
	private int tupleCounter;
	/*the file channel we are using*/
	private FileChannel fcout;
	/*the size of buffer*/
	private static int size = 4096;
	/*the size of metadata*/
	private static int metasize = 8;
	/*record the state of meta data initilization*/
	private boolean checkInit;



	//for test
	public static void main(String[] args) throws Exception {
		TupleReader test = new TupleReader("Boats AS B");
		Tuple tuple = test.readNextTuple();
		
		TupleWriter write = new TupleWriter(1);
		
		while (true) {
			if (!write.writeTuple(1, tuple)) {
				
				break;
			}
			tuple = null;
		}
	}
	
	
	//constructor
	//input: index of file 
	public TupleWriter(int index) {
		
		empty = true;
		bufferPosition = 0;
		tupleCounter = 0;
		checkInit= false;
		
		/*create a file and open file channel*/
		StringBuilder output = new StringBuilder(Dynamic_properties.outputPath);
		output.append("/query");	
		output.append(String.valueOf(index));

		File file = new File(output.toString());
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		try {
			fcout = new RandomAccessFile(file, "rw").getChannel();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/*allocate buffer for writing*/
		buffer = ByteBuffer.allocate(size);
		
		
	}
	
	//write tuples
	public boolean writeTuple(int index, Tuple tuple) throws IOException {
		
		/*the end of write operation, the last page*/
		if (tuple == null) {
			/*if there are still severl tuples in the last page*/
			if(!empty) {
				clear(bufferPosition);
				buffer.putInt(4, tupleCounter);
				fcout.write(buffer);
				empty = true;
				bufferPosition = 0;
			}	
			
			close();
			return false;
		}
		
		/*get meta data from the first tuple*/
		if (!checkInit) {
			attributeNumber = tuple.getSchema().keySet().size();
			buffer.limit();
			System.out.println(buffer.limit() - metasize);
			System.out.println(attributeNumber * 4);
			maxTupleNumber = (int)((buffer.limit() - metasize)/(attributeNumber * 4));
			checkInit = true;
		}
		
		/*if it is a new buffer*/
		if (empty) {
			initMetaData();
			System.out.println();
			empty = false;
		}
		
		
		/*check the space of buffer*/
		if (tupleCounter != maxTupleNumber) {
			
			/*write tuple to buffer*/
			for (int i=0; i<attributeNumber; i++) {
				buffer.putInt(bufferPosition, (int)tuple.getData()[i]);
				bufferPosition +=4;
			}
			tupleCounter +=1;
			
		} else {
			
			/*zero out the rest space in buffer and flush it to channel*/
			clear(bufferPosition);
			fcout.write(buffer);
			empty = true;
			bufferPosition = 0;
			
		}

		
		return true;
	}
	
	private void initMetaData() {
		/*attribute numbers*/
		buffer.putInt(bufferPosition, attributeNumber);
		bufferPosition+=4;
		/*tuple numbers*/
		buffer.putInt(bufferPosition, maxTupleNumber);
		bufferPosition+=4;
	}
	 
	/**
	 * zero out all positions between bufferPosition and limit
	 * 
	 * @param bufferPosition
	 */
	
	private void clear(int bufferPosition){
		
		int times = buffer.limit() - bufferPosition;
		for (int i=0; i< times; i++) {
			buffer.put(bufferPosition, (byte) 0);
			bufferPosition++;
		}
		
	}
	
	
	/**
	 * close the file channel
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {

		try {
			if(fcout != null) {
				fcout.close();
			}

		} catch(IOException e) {
			throw e;
		}

	}

}
