package data;

/**
 * 
 * This class provides function:
 * 
 * store dynamic properties for future use
 * 
 * @author Yixuan Jiang
 *
 */
public class Dynamic_properties {

	public static String inputPath = "src/samples/input";
	public static String dataPath = inputPath + "/db/data/";
	public static String queryPath = inputPath + "/queries.sql";
	public static String schemaPath = inputPath + "/db/schema.txt";
	public static String outputPath = "src/samples/output";
	
	/**
	 * set input and output path according to pass in parameters
	 * @param p0 input absolute path
	 * @param p1 output absolute path
	 */
	public static void setPath(String p0, String p1) {
		
		inputPath = p0;
		outputPath = p1;
		dataPath = inputPath + "/db/data/";
		queryPath = inputPath + "/queries.sql";
		schemaPath = inputPath + "/db/schema.txt";
		
	}
	
}
