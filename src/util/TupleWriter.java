package util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
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
	private BufferedWriter humanbw;



	//for test
//	public static void main(String[] args) throws Exception {
//		TupleReader test = new TupleReader("Boats AS B");
//		Tuple tuple = test.readNextTuple();
//
//		TupleWriter write = new TupleWriter(Dynamic_properties.outputPath+"/query1");
//		while (true) {
//			if(!write.writeTuple(tuple)) {
//				break;
//			}
//			tuple = test.readNextTuple();
//		}
//
//	}


	public TupleWriter(String path) {

		StringBuilder output = new StringBuilder(path);

		/**********************init for binary file********************************/
		
		empty = true;
		bufferPosition = 0;
		tupleCounter = 0;
		checkInit= false;

		/*create a file and open file channel*/

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


		/**********************init for human readable file********************************/
		output.append("_humanreadable");

		file = new File(output.toString());
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		try {
			humanbw = new BufferedWriter(new FileWriter(file));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

	//write tuples
	public boolean writeTuple(Tuple tuple) throws IOException {


		writeReadableTuple(tuple);
		return  writeBinaryTuple(tuple);

	}

	public void writeReadableTuple(Tuple tuple) throws IOException {

		//如果为null 返回false
		if (tuple != null) {
			tuple.printData();
			humanbw.write(tuple.getTupleData().toString() + '\n');
		}

	}

	public boolean writeBinaryTuple(Tuple tuple) throws IOException {

		/*the end of write operation, the last page*/
		if (tuple == null) {
			/*if there are still several tuples in the last page*/
			if(!empty) {
				clear(bufferPosition);
				buffer.putInt(4, tupleCounter);
				fcout.write(buffer);


				empty = true;
				bufferPosition = 0;
				tupleCounter =0;
			}	

			//要在这里close吗？
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
			empty = false;
		}


		/*check the space of buffer*/
		//if (tupleCounter != maxTupleNumber) {

		/*write tuple to buffer*/
		for (int i=0; i<attributeNumber; i++) {
			buffer.putInt(bufferPosition, (int)tuple.getData()[i]);
			bufferPosition +=4;
		}
		tupleCounter +=1;
		if (tupleCounter == maxTupleNumber) {
			/*zero out the rest space in buffer and flush it to channel*/
			clear(bufferPosition);
			fcout.write(buffer);

			empty = true;
			bufferPosition = 0;
			tupleCounter =0;
		}

		//} else {



		//}


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

		buffer.clear();

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

			if (humanbw != null) {
				humanbw.close();
			}

		} catch(IOException e) {
			throw e;
		}

	}

}
