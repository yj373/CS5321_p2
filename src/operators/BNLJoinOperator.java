package operators;

import data.Tuple;
import net.sf.jsqlparser.expression.Expression;

/**
 * This class provides function:
 * 
 * Doing Block Nested Loop Join
 * 
 * 
 * @author Xiaoxing Yan
 */


public class BNLJoinOperator extends JoinOperator{


	/*buffer for storing Tuples*/
	private Tuple[] bufferTuples;
	/*the size of one page in bytes*/
	private static int size = 4096;
	/*the next tuple's index*/
	private int outerTupleIndex;
	/*Tuple read from inner relation*/
	private Tuple innerTuple;
	/*check whether a new block needed to be loaded*/
	private boolean reFillBuffer;
	/*check whether to read a new inner tuple or not*/
	private boolean needInnerTuple;
	/*check whether a block left for outer reltion */
	private boolean blockLeft;
	/*maximum tuples in the current block*/
	private int maxTupleNumber;
	
	/*variables to check the empty table*/
	private boolean initLeftTable;
	private boolean leftTableEmpty;
	private boolean initRightTable;
	private boolean rightTableEmpty;
	


	/** 
	 * This method is a constructor which is to
	 * initialize related fields.
	 * 
	 * @param op1 left operator
	 * @param op2 right operator
	 * @param expression
	 * @param bufferPage number of pages in a buffer
	 * 
	 */
	public BNLJoinOperator(Operator op1, Operator op2, Expression expression, int bufferPage) {

		super(op1, op2, expression);
		
		StringBuilder sb = new StringBuilder();
		sb.append("bnlj-");
		sb.append(op1.name);
		sb.append("-");
		sb.append(op2.name);
		name = sb.toString();

		/*calculate the number of tuples in this buffer and prepare buffer*/
		int numberTuples = (int)(bufferPage * size)/(op1.schema.size() *4);
		
		bufferTuples = new Tuple[numberTuples];
		outerTupleIndex = 0;
		reFillBuffer = true;
		needInnerTuple = true;
		blockLeft = true;
		maxTupleNumber = 0;
	
		initLeftTable = false;
		leftTableEmpty = false;
		initRightTable =false;
		rightTableEmpty =false;

	}


	/**
	 * the method provides function:
	 * 
	 * refill blocks 
	 */
	public void fillBuffer () {

		maxTupleNumber = 0;
		for(int i=0; i< bufferTuples.length; i++) {
			Tuple temp = leftChild.getNextTuple();
			if (temp != null) {
				bufferTuples[i] = temp;
				maxTupleNumber++;
			} else {
				/*read all tuples in outer relation*/
				blockLeft = false;
				break;
			}
		}
		
		/*check the empty state of left table*/
		if (!initLeftTable) {
			initLeftTable = true;
			if (maxTupleNumber == 0) {
				leftTableEmpty = true;
			}
		}
	}

	/**
	 * @return the Tuple joined from the leftChild Operator and rightChild Operator.
	 */
	@Override
	public Tuple getNextTuple(){

		/* corner case 1: if joinCondition is null, BNJ join is the same as TNLJ. */
		if (exp == null) {
			return super.getNextTuple();
		}

		/* corner case 2: Since join condition is not null, as long as there 
		 * is one empty table, return null */		
		if (leftTableEmpty || rightTableEmpty) {
			return null;
		}

		while (true) {

			/*fill the buffer for outer relation table*/
			if (reFillBuffer) {
				fillBuffer ();
				reFillBuffer = false;
			}

			/*read one tuple from inner relation table*/
			if (needInnerTuple) {
				innerTuple = rightChild.getNextTuple();
				needInnerTuple = false;
				
				/*check the empty state of right table*/
				if (!initRightTable) {
					initRightTable = true;
					if (innerTuple == null ) {
						rightTableEmpty = true;
					}
				}
			}

			
			if (leftTableEmpty || rightTableEmpty) {
				return null;
			}
			
			
			Tuple res = null;
			Tuple leftTuple = null;
			if (outerTupleIndex < maxTupleNumber) {
				leftTuple = bufferTuples[outerTupleIndex];
			}

			Tuple rightTuple = innerTuple;

			if (rightTuple == null) {

				/*case 1: for one block, have iterated all tuples within inner relation*/
				if (blockLeft) {

					reFillBuffer = true;
					outerTupleIndex = 0;
					rightChild.reset();
					needInnerTuple = true;

					continue;

				} else {
					/*case 2: have iterated all tuples within outer and inner relation*/
					return null;
				}

			} else {

				/*case 3: for one block, keep joining tuples with current inner tuple*/
				if (outerTupleIndex < maxTupleNumber) {
					outerTupleIndex ++;
				} else {

					/*case 4: for one block, read the next inner tuple and join*/
					outerTupleIndex = 0;
					needInnerTuple = true;
					continue;
				}

			}

			/*if the tuple has passed the join condition, return it*/
			res = concatenate(leftTuple, rightTuple);
			if (judgeExpression(res)) {
				return res;
			} 
		}


	}
}
