package test;

import java.io.StringReader;

import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import operators.JoinOperator;
import operators.ProjectOperator;
import operators.ScanOperator;
import operators.SelectOperator;
import operators.SortOperator;

/**
 * This class provides function:
 * Testing for methods in JoinOperator
 * 
 * @author Ruoxuan Xu
 *
 */
class JoinOperatorTest {
	
	/**
	 * test method getNextTuple()
	 * 
	 * @throws JSQLParserException
	 */
	@Test
	public void getNextTupleTest() throws JSQLParserException {

		String[] statements = new String[5];
		
		statements[0] = "SELECT * FROM Sailors S, Reserves R WHERE S.B = 300;";
		statements[1] = "SELECT * FROM Sailors S1, Sailors S2 WHERE S1.B = 100 AND S2.B > 100;";
		statements[2] = "SELECT * FROM Sailors S, Reserves R WHERE S.A = R.G;";
		statements[3] = "SELECT * FROM Sailors S, Reserves R WHERE S.B = 100 AND R.G < 2 ;";
		statements[4] = "SELECT * FROM Sailors S, Boats B WHERE S.B = 100 AND B.E > 1;";
		
		for (int i=0; i< statements.length; i++) {
			System.out.println("*******when statement is : " + statements[i]);
			String statement = statements[i];
			CCJSqlParserManager parserManager = new CCJSqlParserManager();
			PlainSelect ps = (PlainSelect) ((Select) parserManager.
					parse(new StringReader(statement))).getSelectBody();
			
			String table_info = ps.getFromItem().toString();
			String joinTableInfo = ps.getJoins().get(0).toString();
			
			ScanOperator scanOp1 = new ScanOperator(table_info);
			ScanOperator scanOp2 = new ScanOperator(joinTableInfo);
			
			JoinOperator joinOp = new JoinOperator(scanOp1, scanOp2);
			SelectOperator selectOp = new SelectOperator(ps, joinOp);
			
			ProjectOperator projectOp = new ProjectOperator(ps, selectOp);
			projectOp.dump();
	
			System.out.println("*******end*********");
			System.out.println();
		}

	}

}