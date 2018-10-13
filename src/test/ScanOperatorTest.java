package test;

import operators.ProjectOperator;
import operators.ScanOperator;
import operators.SelectOperator;
import data.Tuple;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.LinkedList;

/**
 * This class provides function:
 * Testing for methods in SelectOperator
 * 
 * @author Ruoxun Xu
 *
 */
class ScanOperatorTest {


	/**Test for Constructor: ScanOperator(String tableInfo)*/
	@Test 
	void testForAlias() {
		String tableInfo = "Sailors S1";
		ScanOperator sOp = new ScanOperator(tableInfo);
		assertEquals(sOp.getTableName(), "Sailors");
		assertEquals(sOp.getTableAliase(), "S1");
		
		LinkedList<String> schema = new LinkedList<>();
		schema.add("A");
		schema.add("B");
		schema.add("C");
		
		for (int i = 0; i < schema.size(); i++) {
			assertEquals(sOp.getAttributes().get(i), schema.get(i));
		}
		assertEquals(sOp.getAttributes().size(), schema.size());		
	}
	
	/**Test for Constructor: ScanOperator(String tableName, String tableAliase)*/
	@Test
	void testForExplicitAlias() {
		String tableName = "Reserves";
		String tableAliase = "R";
		ScanOperator sOp= new ScanOperator(tableName, tableAliase);
			
		LinkedList<String> schema = new LinkedList<>();
		schema.add("G");
		schema.add("H");
		
		for (int i = 0; i < schema.size(); i++) {
			assertEquals(sOp.getAttributes().get(i), schema.get(i));
		}
		assertEquals(sOp.getAttributes().size(), schema.size());
	}
	
	/**Test for getNextTuple() and reset()*/
	@Test
	void testForTuple() {
		ScanOperator sOp = new ScanOperator("Sailors S");
		
		Tuple tuple = sOp.getNextTuple();
		assertEquals(tuple.getTupleData(), "1 200 50 ");
		
		tuple = sOp.getNextTuple();
		assertEquals(tuple.getTupleData(), "2 200 200 ");
		
		tuple = sOp.getNextTuple();
		assertEquals(tuple.getTupleData(), "3 100 105 ");
		
		tuple = sOp.getNextTuple();
		assertEquals(tuple.getTupleData(), "4 100 50 ");
		
		tuple = sOp.getNextTuple();
		assertEquals(tuple.getTupleData(), "5 100 500 ");
		
		tuple = sOp.getNextTuple();
		assertEquals(tuple.getTupleData(), "6 300 400 ");
		
		tuple = sOp.getNextTuple();
		assertEquals(tuple, null);
		
		sOp.reset();
		assertEquals(sOp.getNextTuple().getTupleData(), "1 200 50 ");		
	}
	
	/** Test SQL*/
	@Test
	void testForSQL() {
		String[] statements = new String[5];	
		statements[0] = "SELECT Sailors.A, Sailors.B FROM Sailors;";
		statements[1] = "SELECT Sailors.B, Sailors.A FROM Sailors;";
		statements[2] = "SELECT * FROM Sailors;";
		statements[3] = "SELECT * FROM Reserves;";
		statements[4] = "SELECT * FROM Boats;";
		
		try {
			for (int i=0; i< statements.length; i++) {
				System.out.println("*******when statement is : " + statements[i]);
				String statement = statements[i];
				CCJSqlParserManager parserManager = new CCJSqlParserManager();
				PlainSelect ps;
				
				ps = (PlainSelect) ((Select) parserManager.parse(new StringReader(statement))).getSelectBody();
		
				String table_info = ps.getFromItem().toString();
				ScanOperator scanOp = new ScanOperator(table_info);
				scanOp.dump();
				System.out.println("*******end*********");
				System.out.println();
			}
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}
		
	}
	
	

}
