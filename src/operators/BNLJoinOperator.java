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


	public BNLJoinOperator(Operator op1, Operator op2, Expression expression, int bufferPage) {

		super(op1, op2, expression);

		/*calculate the number of tuples in this buffer and prepare buffer*/
		int numberTuples = (int)(bufferPage * size)/(op1.schema.size() *4);
		bufferTuples = new Tuple[numberTuples];
		outerTupleIndex = 0;

		reFillBuffer = true;
		needInnerTuple = true;
		blockLeft = true;

	}

	public void fillBuffer () {

		for(int i=0; i< bufferTuples.length; i++) {
			Tuple temp = leftChild.getNextTuple();
			if (temp != null) {
				bufferTuples[i] = temp;
			} else {
				/*read all tuples in outer relation*/
				blockLeft = false;
				break;
			}

		}

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
			if (outerTupleIndex < bufferTuples.length) {
				leftTuple = bufferTuples[outerTupleIndex];
			}

			Tuple rightTuple = innerTuple;

			//如果outer 还有tuple 右边没到底  -- 左边读一个 右边不变
			//如果outer 没有tuple 并且还有block 右边没到底 --左边重置 右边读一个
			//如果outer 没有tuple 并且还有block 右边到底了 -- 左边读一个block 右边重置
			//如果outer 没有tuple 没有block 右边到底了 -- 返回null
			if (outerTupleIndex < bufferTuples.length && rightTuple !=null ) {
				outerTupleIndex ++;
			} else if (outerTupleIndex >= bufferTuples.length && rightTuple !=null) {
				outerTupleIndex = 0;
				needInnerTuple = true;
				continue;
			} else if (outerTupleIndex >= bufferTuples.length && blockLeft) {
				reFillBuffer = true;
				rightChild.reset();
				
				
				//null的情况！！！！没有reset成功
				outerTupleIndex = 0;
				continue;
			} else if(outerTupleIndex >= bufferTuples.length && !blockLeft){
				return null;
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
