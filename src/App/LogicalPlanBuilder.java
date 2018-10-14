package App;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import visitors.ExpressionClassifyVisitor;
import visitors.LogicalPlanVisitor;
import logicalOperators.*;

public class LogicalPlanBuilder {
	private PlainSelect ps;
	private LogicalOperator rootOp;
	
	public LogicalPlanBuilder(PlainSelect ps) {
		this.ps = ps;
	}
	
	public LogicalPlanBuilder(Select select) {
		this.ps = (PlainSelect) select.getSelectBody();
	}
	
	public LogicalPlanBuilder(Statement stm) {
		this.ps = (PlainSelect)(((Select)stm).getSelectBody());
	}
	
	public void buildLogicQueryPlan() {	
	// To begin with, build the join operator layer by layer, like a complete binary tree
	    if (ps != null) {
		// first step, set the maintable's scanOperator as the top element
			String mainTableInfo = ps.getFromItem().toString();
			rootOp = new LogicalScanOperator(mainTableInfo);
					
		// Second step, join with all scan operators of join item one by one.
			if (ps.getJoins() != null) {				
				for (Object o : ps.getJoins()) {
		    // join the root with the new coming tables
					rootOp = new LogicalJoinOperator(rootOp, (new LogicalScanOperator(o.toString())));
				}
			}
					
			LogicalOperator projectOp = new LogicalProjectOperator(ps, rootOp);
			LogicalOperator sortOp = new LogicalSortOperator(ps, projectOp);
			LogicalOperator deduplicateOp = new LogicalDuplicateEliminationOperator(ps, sortOp);
			rootOp = deduplicateOp;
		}
	    // To end with, insert additional expressions into these operators
		addQueryCondition();
	}
	
		
	// add conditions to the former built joinQueryPlan, only called inside buildQueryPlan() 
	private void addQueryCondition() {
		// get the expression conditions from ps.
		ExpressionClassifyVisitor classifier = new ExpressionClassifyVisitor();
		classifier.classify(ps);
		
		// traverse from top to the bottom of the query plan, put conditions to its place.
		LogicalPlanVisitor Venture = new LogicalPlanVisitor(classifier);
		if (rootOp != null) {
	        rootOp.accept(Venture);
		}
	}
	
	// get the root Operator of the query plan
	LogicalOperator getRoot() {
		return rootOp;
	}
}

