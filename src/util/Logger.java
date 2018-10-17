package util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import data.Dynamic_properties;

public class Logger {
    private static Logger loggerInstance = null;
    private String fileDiretory;
    private FileWriter fileWriter;
 
    // Singleton pattern, only one loggerInstance is allowed.   
    private Logger() throws IOException {
    	this.fileDiretory = Dynamic_properties.outputPath;
    	File folder = new File(fileDiretory);
		if (!folder.exists()) {
			folder.mkdirs();
		}
    	File file = new File(fileDiretory + "/log");
    	fileWriter = new FileWriter(file);
    }
   
    // Logger.println() to add 
    public static void println(String debugLine) {
    	try {
	    	if (loggerInstance == null) {
	    		loggerInstance = new Logger();
	    	} else {
	    		BufferedWriter bw = new BufferedWriter(loggerInstance.fileWriter);
	    		bw.write(debugLine + '\n');
	    		bw.close();
	    	}	    	
    	} catch (IOException e) {
    		e.printStackTrace();
			e.getMessage();
    	}  	
    }
    
    public static void print(String debugLine) {
    	try {
	    	if (loggerInstance == null) {
	    		loggerInstance = new Logger();
	    	} else {
	    		BufferedWriter bw = new BufferedWriter(loggerInstance.fileWriter);
	    		bw.write(debugLine);
	    		bw.close();
	    	}	    	
    	} catch (IOException e) {
    		e.printStackTrace();
			e.getMessage();
    	}  	
    }
    
    
}
