package data;

import operators.Operator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;  
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class TupleReader {

	/*the next position we want to read from the buffer*/
	private int bufferPosition;
	/*the next position we want to read from the file to channel*/
	private long filePosition;
	/*check whether the buffer is empty or not*/
	private boolean empty;
	/*the number of tuples we can read in the buffer or page*/
	private int number;
	/*buffer store bytes in one page*/
	private ByteBuffer buffer;
	private String tableInfo;
	private int attributeNumber;
	private int tupleNumber;


	public TupleReader (String tableInfo) {
		try {
			empty = true;
			filePosition = 0;
			bufferPosition = 0;
			this.tableInfo = new String(tableInfo);//debug later
			/*create a new buffer with size 4096 bytes*/ 
			buffer = ByteBuffer.allocate(4096); 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Tuple readNextTuple(Operator op) throws Exception {

		/*if buffer is empty, it needs to load another page*/
		if (empty) {
			int r = readFromChannel(tableInfo);
			// read方法返回读取的字节数，可能为零，如果该通道已到达流的末尾，则返回-1  
			/*if we have reached the end of file*/
			if (r == -1) {  
				return null;  
			} 
			empty = false;
		} 
		
		/*initilize metadata if this buffer read a new page*/
		checkBuffer();
		
		//getInt from buffer
		//create new Tuple
		long[] data = new long[attributeNumber];
		for (int i=0; i<attributeNumber; i++) {
			data[i] = (long)buffer.getInt(bufferPosition);
			bufferPosition = buffer.position();
			
		}
		Tuple tuple = new Tuple();
		tupleNumber--;
		if (tupleNumber == 0) {
			empty = true;
		}
		
		//这个build new tuple 怎么改
		return tuple;
	}

	public void checkBuffer() {
		
		if (buffer.position() < 8) {
			/*initilize metadata*/
			attributeNumber = buffer.getInt(bufferPosition);
			bufferPosition = buffer.position();
			tupleNumber = buffer.getInt(bufferPosition);
			bufferPosition = buffer.position();	
		}
	}

	private int readFromChannel (String tableInfo) throws Exception  {
		
	

		//get the path of file -- correct later
		String infile = "C:\\copy.sql";  
		String outfile = "C:\\copy.txt"; 

		/*get input stream of the source file*/ 
		FileInputStream fin = new FileInputStream(infile);  

		/*get the channel of source file*/ 
		FileChannel fcin = fin.getChannel();  

		// clear方法重设缓冲区，使它可以接受读入的数据  
		buffer.clear();  

		/*Reads a sequence of bytes from this channel into the given buffer.*/
		fcin.position(filePosition);
		int r = fcin.read(buffer); 
		/*record the next position*/
		filePosition = fcin.position()+1;//debug later
		buffer.flip();//将position置0
		
		return r;
	}



	public void close() {

	}

	public void reset() {

	}

}



