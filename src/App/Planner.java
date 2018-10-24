package App;

import data.Dynamic_properties;
import java.io.FileReader;
import java.util.logging.Level;

import util.GlobalLogger;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import operators.Operator;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * This class provides function:
 * 
 * for future optimization
 * 
 * @author Yixuan Jiang
 *
 */
public class Planner {
	
	/**
	 * to parse queries
	 */
	public void parse_queries() {
		try {
			CCJSqlParser parser = new CCJSqlParser(new FileReader(Dynamic_properties.queryPath));
			Statement statement;
			while ((statement = parser.Statement()) != null) {
				GlobalLogger.getLogger().info("Read statement: " + statement);
				//System.out.println("Read statement: " + statement);
				Select select = (Select) statement;
				PlainSelect ps = (PlainSelect)select.getSelectBody();
				Operator op = generatePlan(ps);
				GlobalLogger.getLogger().info("Select body is " + select.getSelectBody());
				//System.out.println("Select body is " + select.getSelectBody());
			}
		} catch (Exception e) {
			//System.err.println("Exception occurred during parsing");
			GlobalLogger.getLogger().log(Level.SEVERE, e.toString(), e);
			e.printStackTrace();
		}
	}
	
	/**
	 * generate query plan
	 * 
	 * @param ps
	 * @return operator root node
	 */
	public Operator generatePlan(PlainSelect ps) {
		return null;
	}

}
