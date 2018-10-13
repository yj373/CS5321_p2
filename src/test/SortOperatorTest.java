package test;


import java.io.StringReader;
import org.junit.jupiter.api.Test;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import operators.ProjectOperator;
import operators.ScanOperator;
import operators.SelectOperator;
import operators.SortOperator;


/**
 * This class provides function:
 * Testing for methods in SortOperator
 * 
 * @author Xiaoxing Yan
 *
 */
class SortOperatorTest {

	/**
	 * test method getNextTuple()
	 * 
	 * @throws JSQLParserException
	 */
	
	@Test
	public void getNextTupleTest() throws JSQLParserException {

		String[] statements = new String[4];
		
		statements[0] = "SELECT * FROM Sailors;";
		statements[1] = "SELECT * FROM Sailors S ORDER BY S.B;";
		statements[2] = "SELECT S.B, S.C FROM Sailors S Order By S.B;";
		statements[3] = "SELECT * FROM Sailors WHERE Sailors.B = 100 ORDER BY Sailors.C;";
		
		
		for (int i=0; i< statements.length; i++) {
			System.out.println("*******when statement is : " + statements[i]);
			String statement = statements[i];
			CCJSqlParserManager parserManager = new CCJSqlParserManager();
			PlainSelect ps = (PlainSelect) ((Select) parserManager.
					parse(new StringReader(statement))).getSelectBody();
			String table_info = ps.getFromItem().toString();
			ScanOperator scanOp = new ScanOperator(table_info);
			SelectOperator selectOp = new SelectOperator(ps,scanOp);
			
			SortOperator sortOp = new SortOperator(ps, selectOp);
			ProjectOperator projectOp = new ProjectOperator(ps, sortOp);
			projectOp.dump();
			System.out.println("*******end*********");
			System.out.println();

		}

	}



}
