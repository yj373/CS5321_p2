package operators;

import data.Dynamic_properties;
import data.Tuple;
/**
 * The format of temporary scratch file naming: aliase_column_passNum_queryNum
 * @author Yixuan Jiang
 *
 */
public class ExternalSort extends Operator{
	
	private int queryNum;
	private String tempDirPath;
	
	
	public ExternalSort(int queryNumber, Operator op) {
		setLeftChild(op);
		this.queryNum = queryNumber;
		this.tempDirPath = Dynamic_properties.tempPath;
	}
	
	public void checkTempDirectory() {
		
	}

	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}
	
	//Test
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
