package App;

import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;

import data.Dynamic_properties;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.select.Select;
import operators.Operator;
import util.GlobalLogger;
import visitors.PhysicalPlanVisitor;


/**
 * This class provides function:
 * 
 * Interpret query to query plan
 * evaluate query plan and generate output files
 * 
 * @author Xiaoxing Yan
 *
 */
public class SQLInterpreter {

	/**
	 * Set paths according to input parameters
	 * 
	 * @param args   absolute paths
	 * @param args[0]   absolute path of input file
	 * @param args[1]   absolute path of output file
	 * @param args[2]   absolute path of temporary scratch file directory
	 */
	public static void init (String[] args) {
		if(args.length==3) {
			Dynamic_properties.setPath(args[0], args[1], args[2]);
		}else if (args.length == 2) {
			Dynamic_properties.setPath(args[0], args[1]);
		}
		
	}
	

	/**
	 * Build query tree for every query
	 */
	public static void BuildQueryPlan () {
		
		String queriesFile = Dynamic_properties.queryPath;
		try {
			Operator root = null;
			CCJSqlParser parser = new CCJSqlParser(new FileReader(queriesFile));
			net.sf.jsqlparser.statement.Statement statement;
			int index = 1;

			while ((statement = parser.Statement()) != null) {

				/*calculate spend time*/
				long startTime=System.currentTimeMillis();    
				GlobalLogger.getLogger().info("TIME START " + startTime);
				long endTime = 0;
				
				
				
				GlobalLogger.getLogger().info("Read statement: " + statement);
				
				//System.out.println("Read statement: " + statement);
				Select select = (Select) statement;
				LogicalPlanBuilder lb = new LogicalPlanBuilder(select);
				lb.buildLogicQueryPlan();
				PhysicalPlanVisitor pv = new PhysicalPlanVisitor(index);
				try {
					lb.getRoot().accept(pv);
					root = pv.getPhysicalRoot();
					
					/*get the ending time*/
					endTime=System.currentTimeMillis();  
					writeToFile (index, root);
					GlobalLogger.getLogger().info("time spent： "+(endTime - startTime)+"ms"); 
		
					
				} catch (Exception e) {
					//System.err.println("Exception occurred during paring query" + index);
					GlobalLogger.getLogger().log(Level.SEVERE, e.toString(), e);
			        e.printStackTrace();
				}
				index++;	
			}

		} catch (Exception e){
			 //System.err.println("Exception occurred during parsing");
			 GlobalLogger.getLogger().log(Level.SEVERE, e.toString(), e);
	         e.printStackTrace();
		}

	}
	
	/**
	 * write output to output files
	 * 
	 * @param index index of query
	 * @param root  root node of query tree
	 * @throws IOException 
	 */

	public static void writeToFile (int index, Operator root) throws IOException {
		root.dump(index);
		GlobalLogger.getLogger().info("end");
		//System.out.println("end");
	}
	
}
