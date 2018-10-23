package util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import data.Dynamic_properties;

/**
 * Debug Tool: to generate log files to maintain the output during debugging and running
 * process of the program.
 * 
 * @author Ruoxuan Xu
 *
 */
public class Logger {
	/* ingleton pattern, only one loggerInstance is allowed */
    private static Logger loggerInstance = null;
    
    /* non static fields are all fields of the single instance */
    private String fileDiretory;
    private FileWriter fileWriter;
 
    /**
     * private constructor to create the only instance of this class.
     * The log file remains the same wherever it is called.
     * @throws IOException
     */
    private Logger() throws IOException {
    	this.fileDiretory = Dynamic_properties.outputPath;
    	File folder = new File(fileDiretory);
		if (!folder.exists()) {
			folder.mkdirs();
		}
    	File file = new File(fileDiretory + "/log");
    	// if file doesnt exists, then create it
        if (!file.exists()) {
            file.createNewFile();
        }
    	fileWriter = new FileWriter(file);
    }
   
    /**
     * Write debugLine into the log file with the '\n' as the default end.
     * @param debugLine
     */
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
    
    /**
     * Write debugLine into the log file without the '\n' as the default end.
     * @param debugLine
     */
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
