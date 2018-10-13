package test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.StringReader;

import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import operators.DuplicateEliminationOperator;
import operators.ProjectOperator;
import operators.ScanOperator;
import operators.SelectOperator;
import operators.SortOperator;

/**
 * This class provides function:
 * Testing for methods in DuplicateEliminationOperator
 * 
 * @author Xiaoxing Yan
 *
 */

class DuplicateEliminationOperatorTest {

	
	/**
	 * test method getNextTuple()
	 * 
	 * @throws JSQLParserException
	 */
	@Test
	public void getNextTupleTest() throws JSQLParserException {
		String[] statements = new String[1];
		statements[0] = "SELECT DISTINCT * FROM Reserves R WHERE R.H = 101 ORDER BY R.H;";


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
			System.out.println();
			DuplicateEliminationOperator dupOp = new DuplicateEliminationOperator(ps, projectOp);
			dupOp.dump();
			System.out.println("*******end*********");
			System.out.println();
		}
	}

}