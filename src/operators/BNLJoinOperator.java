package operators;

import java.util.Map;

import data.Tuple;
import net.sf.jsqlparser.expression.Expression;

public class BNLJoinOperator extends JoinOperator{

	//	private Operator leftChild;
	//	private Operator rightChild;
	//	private Expression exp;

	private Tuple[] bufferTuples;
	/*the size of one page in bytes*/
	private static int size = 4096;
	/*the next tuple's index*/
	private int outerTupleIndex;
	
	

	private Tuple innerTuple;


	private boolean reFillBuffer;
	private boolean needInnerTuple;
	private boolean blockLeft;
	
	/*maximum tuples in the current block*/
	private int maxTupleNumber;

	//for test 
	private int blocknum;


	public BNLJoinOperator(Operator op1, Operator op2, Expression expression, int bufferPage) {

		super(op1, op2, expression);

		/*calculate the number of tuples in this buffer and prepare buffer*/
		int numberTuples = (int)(bufferPage * size)/(op1.schema.size() *4);
		bufferTuples = new Tuple[numberTuples];
		outerTupleIndex = 0;

		reFillBuffer = true;
		needInnerTuple = true;
		blockLeft = true;

		maxTupleNumber = 0;
		blocknum = 0;

	}

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
		//test
		System.out.println("现在是block是第几个 "+blocknum);
		blocknum ++;

	}

	@Override
	public Tuple getNextTuple(){

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
			}

			Tuple res = null;
			Tuple leftTuple = null;
			if (outerTupleIndex < maxTupleNumber) {
				leftTuple = bufferTuples[outerTupleIndex];
			}

			Tuple rightTuple = innerTuple;

//			//如果outer 还有tuple 右边没到底  -- 左边读一个 右边不变
//			//如果outer 没有tuple 并且还有block 右边没到底 --左边重置 右边读一个
//			//如果outer 没有tuple 并且还有block 右边到底了 -- 左边读一个block 右边重置
//			//如果outer 没有tuple 没有block 右边到底了 -- 返回null
//			if (outerTupleIndex < bufferTuples.length && rightTuple !=null ) {
//				outerTupleIndex ++;
//			} 
//
//			//第二种情况
//			if (outerTupleIndex >= bufferTuples.length && rightTuple !=null) {
//				outerTupleIndex = 0;
//				needInnerTuple = true;
//				continue;
//			} 
//
//			//没有处理  -- block重置以后 inner tuple = null的情况
//			//处理第二种情况后的 再读入情况 -- outertupleindex = 0 && inner == null
//			if (rightTuple == null && blockLeft) {
//
//				reFillBuffer = true;
//				rightChild.reset();
//				needInnerTuple = true;
//				outerTupleIndex = 0;
//				continue;
//			} 
			
			
			if (rightTuple == null) {
				
				if (blockLeft) {

					reFillBuffer = true;
					outerTupleIndex = 0;
					
					rightChild.reset();
					needInnerTuple = true;
					
					continue;
				} else {
					return null;
				}
			} else {
				
				if (outerTupleIndex < maxTupleNumber) {
					outerTupleIndex ++;
				} else {
					outerTupleIndex = 0;
					needInnerTuple = true;
					continue;
				}
				
			}
			
	


			/*if we have found the tuple*/
			res = concatenate(leftTuple, rightTuple);
			if (judgeExpression(res)) {
				outerTupleIndex ++;
				return res;
			} 

		}


	}
}
