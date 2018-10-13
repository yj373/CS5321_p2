package test;

import java.io.StringReader;
import org.junit.Test;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import operators.ProjectOperator;
import operators.ScanOperator;
import operators.SelectOperator;

/**
 * This class provides function:
 * Testing for methods in ProjectOperator
 * 
 * @author Xiaoxing Yan
 *
 */

public class ProjectOperatorTest {

	/**
	 * test method getNextTuple()
	 * 
	 * @throws JSQLParserException
	 */
	@Test
	public void getNextTupleTest() throws JSQLParserException {

		String[] statements = new String[5];
		
		statements[0] = "SELECT Sailors.A, Sailors.B FROM Sailors;";
		statements[1] = "SELECT Sailors.B, Sailors.A FROM Sailors;";
		statements[2] = "SELECT * FROM Sailors;";
		statements[3] = "SELECT * FROM Sailors WHERE Sailors.A = 4;";
		statements[4] = "SELECT S.B FROM Sailors S WHERE S.A = 4;";
		
		
		for (int i=0; i< statements.length; i++) {
			System.out.println("*******when statement is : " + statements[i]);
			String statement = statements[i];
			CCJSqlParserManager parserManager = new CCJSqlParserManager();
			PlainSelect ps = (PlainSelect) ((Select) parserManager.
					parse(new StringReader(statement))).getSelectBody();
			String table_info = ps.getFromItem().toString();
			ScanOperator scanOp = new ScanOperator(table_info);
			SelectOperator selectOp = new SelectOperator(ps,scanOp);
			ProjectOperator projectOp = new ProjectOperator(ps, selectOp);
			projectOp.dump();
			System.out.println("*******end*********");
			System.out.println();
			
		}

	}

}