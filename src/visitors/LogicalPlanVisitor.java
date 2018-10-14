package visitors;
import java.util.Map;


import data.TablePair;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import logicalOperators.*;


public class LogicalPlanVisitor {
	private Map<String, Expression> scanConditions;
	private Map<TablePair, Expression> joinConditions;
	
	public LogicalPlanVisitor (ExpressionClassifyVisitor classifier) {
		// get the expression conditions from ps.
		if (classifier != null) {
			this.scanConditions = classifier.getScanConditions();
			this.joinConditions = classifier.getJoinConditions();
		}
	}
	
	public void visit(LogicalScanOperator scOp) {
		String tableAlias = scOp.getTableAliase();
		Expression ex = scanConditions.get(tableAlias);
		if (ex != null) {
			scOp.setExpression(ex);
			scanConditions.remove(tableAlias);
		}
	}
	
	public void visit(LogicalJoinOperator jnOp) {
		LogicalOperator op1 = jnOp.getLeftChild();
		if (op1 != null) {
			op1.accept(this);
		}
		LogicalOperator op2 = jnOp.getRightChild();
		if (op2 != null) {
			op2.accept(this);
		}
		
		Expression expr = null;
		for(TablePair tbpir: joinConditions.keySet()) {
			if (jnOp.getAllTable().contains(tbpir.first()) && jnOp.getAllTable().contains(tbpir.second())) {
				if (expr == null) {
					expr = joinConditions.get(tbpir);
				} else {
					expr = new AndExpression(expr, joinConditions.get(tbpir));
				}
			    joinConditions.remove(tbpir);
			}
		}
		jnOp.setExpression(expr);		
	}
	
	public void visit(LogicalProjectOperator operator) {
		LogicalOperator op1 = operator.getLeftChild();
		if (op1 != null) {
			op1.accept(this);
		}		
	}
	
	public void visit(LogicalSortOperator operator) {
		LogicalOperator op1 = operator.getLeftChild();
		if (op1 != null) {
			op1.accept(this);
		}	
	}

	public void visit(LogicalDuplicateEliminationOperator operator) {
		LogicalOperator op1 = operator.getLeftChild();
		if (op1 != null) {
			op1.accept(this);
		}		
	}
}
