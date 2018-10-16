package App;

/**
 * This class provides function:
 * 
 * Read queries from input files and generate corresponding query plan
 * write outputs to output files
 * 
 * @author Xiaoxing Yan
 *
 */
public class Main {

	public static void main(String[] args) {
		
		SQLInterpreter.init(args);
		SQLInterpreter.BuildQueryPlan();

	}
}
