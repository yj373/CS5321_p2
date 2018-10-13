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

/**
 * This class provides function:
 * Testing for methods in SelectOperator
 * 
 * @author Ruoxun Xu
 *
 */
class SelectOperatorTest {
	@Test
	public void getNextTupleTest() throws JSQLParserException {

		String[] statements = new String[1];
		
		statements[0] = "SELECT * FROM Sailors S WHERE S.B = 200;";
		
		for (int i=0; i< statements.length; i++) {
			System.out.println("*******when statement is : " + statements[i]);
			String statement = statements[i];
			CCJSqlParserManager parserManager = new CCJSqlParserManager();
			PlainSelect ps = (PlainSelect) ((Select) parserManager.
					parse(new StringReader(statement))).getSelectBody();
			String table_info = ps.getFromItem().toString();
			ScanOperator scanOp = new ScanOperator(table_info);
			SelectOperator selectOp = new SelectOperator(ps,scanOp);
			selectOp.dump();

			System.out.println("*******end*********");
			System.out.println();
		}

	}

}