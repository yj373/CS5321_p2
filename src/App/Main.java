package App;

import java.io.FileReader;

import data.Dynamic_properties;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import operators.ScanOperator;

public class Main {

	public static void main(String[] args) {
		String queriesFile = Dynamic_properties.QUERY_PATH;
		try {
			CCJSqlParser parser = new CCJSqlParser(new FileReader(queriesFile));
			Statement statement = parser.Statement();
			Select select = (Select)statement;
			PlainSelect ps = (PlainSelect)select.getSelectBody();
			String table_info = ps.getFromItem().toString();
			ScanOperator scanOp = new ScanOperator(table_info);
			scanOp.dump();
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}

	}

}
