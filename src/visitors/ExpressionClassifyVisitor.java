package visitors;
import java.util.*;

import data.TablePair;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ExpressionClassifyVisitor implements ExpressionVisitor{
	private Map<TablePair, Expression> joinConditions = new HashMap<>();
	private Map<String, Expression> scanConditions = new HashMap<>();
	
	public Map<String, Expression> getScanConditions() {
		return scanConditions;
	}
	
	public Map<TablePair, Expression> getJoinConditions() {
		return joinConditions;
	}
	
	public void classify(PlainSelect ps) {
		if (ps != null) {
			Expression origin = ps.getWhere();
		    if (origin != null) {
		    	origin.accept(this);
		    }
		}
	}

	
	@Override
	public void visit(EqualsTo arg0) {
		// if left node and right node are both columns
		if ((arg0.getLeftExpression() instanceof Column) && 
				(arg0.getRightExpression() instanceof Column)) {
			Column column1 = (Column)arg0.getLeftExpression();
			String[] tableNameIndices = column1.getWholeColumnName().split("\\.");
			String tableAlias1 = tableNameIndices[0];
			
			Column column2 = (Column)arg0.getRightExpression();
			tableNameIndices = column2.getWholeColumnName().split("\\.");
			String tableAlias2 = tableNameIndices[0];
			
			TablePair key = new TablePair(tableAlias1, tableAlias2);
			Expression value = joinConditions.get(key);
			if (value != null) {
				joinConditions.put(key, new AndExpression(value, 
					new EqualsTo(arg0.getLeftExpression(), arg0.getRightExpression())));
			} else {
				joinConditions.put(key, 
					new EqualsTo(arg0.getLeftExpression(), arg0.getRightExpression()));
			}
		} else {
		// In this case, it must have one side to be column and the other to be number
			Column column;
			if (arg0.getLeftExpression() instanceof Column) {
			    column = (Column)arg0.getLeftExpression();
			} else {
				column = (Column)arg0.getRightExpression();
			}
			String[] tableNameIndices = column.getWholeColumnName().split("\\.");
			String tableAlias = tableNameIndices[0];
			
			Expression value = scanConditions.get(tableAlias);
			if(value != null) {
				scanConditions.put(tableAlias, new AndExpression(value, 
					new EqualsTo(arg0.getLeftExpression(), arg0.getRightExpression())));
			} else {
				scanConditions.put(tableAlias,
				new EqualsTo(arg0.getLeftExpression(), arg0.getRightExpression()));
			}
		}		
	}
	
	@Override
	public void visit(GreaterThan arg0) {
		// if left node and right node are both columns
		if ((arg0.getLeftExpression() instanceof Column) && 
				(arg0.getRightExpression() instanceof Column)) {
			Column column1 = (Column)arg0.getLeftExpression();
			String[] tableNameIndices = column1.getWholeColumnName().split("\\.");
			String tableAlias1 = tableNameIndices[0];
			
			Column column2 = (Column)arg0.getRightExpression();
			tableNameIndices = column2.getWholeColumnName().split("\\.");
			String tableAlias2 = tableNameIndices[0];
			
			TablePair key = new TablePair(tableAlias1, tableAlias2);
			Expression value = joinConditions.get(key);
			if (value != null) {
				joinConditions.put(key, new AndExpression(value, 
					new GreaterThan(arg0.getLeftExpression(), arg0.getRightExpression())));
			} else {
				joinConditions.put(key, 
					new GreaterThan(arg0.getLeftExpression(), arg0.getRightExpression()));
			}
		} else {
		// In this case, it must have one side to be column and the other to be number
			Column column;
			if (arg0.getLeftExpression() instanceof Column) {
			    column = (Column)arg0.getLeftExpression();
			} else {
				column = (Column)arg0.getRightExpression();
			}
			String[] tableNameIndices = column.getWholeColumnName().split("\\.");
			String tableAlias = tableNameIndices[0];
			
			Expression value = scanConditions.get(tableAlias);
			if(value != null) {
				scanConditions.put(tableAlias, new AndExpression(value, 
					new GreaterThan(arg0.getLeftExpression(), arg0.getRightExpression())));
			} else {
				scanConditions.put(tableAlias,
				new GreaterThan(arg0.getLeftExpression(), arg0.getRightExpression()));
			}
		}		
	}

	
    @Override
	public void visit(GreaterThanEquals arg0) {
		// if left node and right node are both columns
		if ((arg0.getLeftExpression() instanceof Column) && 
				(arg0.getRightExpression() instanceof Column)) {
			Column column1 = (Column)arg0.getLeftExpression();
			String[] tableNameIndices = column1.getWholeColumnName().split("\\.");
			String tableAlias1 = tableNameIndices[0];
			
			Column column2 = (Column)arg0.getRightExpression();
			tableNameIndices = column2.getWholeColumnName().split("\\.");
			String tableAlias2 = tableNameIndices[0];
			
			TablePair key = new TablePair(tableAlias1, tableAlias2);
			Expression value = joinConditions.get(key);
			if (value != null) {
				joinConditions.put(key, new AndExpression(value, 
					new GreaterThanEquals(arg0.getLeftExpression(), arg0.getRightExpression())));
			} else {
				joinConditions.put(key, 
					new GreaterThanEquals(arg0.getLeftExpression(), arg0.getRightExpression()));
			}
		} else {
		// In this case, it must have one side to be column and the other to be number
			Column column;
			if (arg0.getLeftExpression() instanceof Column) {
			    column = (Column)arg0.getLeftExpression();
			} else {
				column = (Column)arg0.getRightExpression();
			}
			String[] tableNameIndices = column.getWholeColumnName().split("\\.");
			String tableAlias = tableNameIndices[0];
			
			Expression value = scanConditions.get(tableAlias);
			if(value != null) {
				scanConditions.put(tableAlias, new AndExpression(value, 
					new GreaterThanEquals(arg0.getLeftExpression(), arg0.getRightExpression())));
			} else {
				scanConditions.put(tableAlias,
				new GreaterThanEquals(arg0.getLeftExpression(), arg0.getRightExpression()));
			}
		}		
	}
	
	@Override
	public void visit(MinorThan arg0) {
		// if left node and right node are both columns
		if ((arg0.getLeftExpression() instanceof Column) && 
				(arg0.getRightExpression() instanceof Column)) {
			Column column1 = (Column)arg0.getLeftExpression();
			String[] tableNameIndices = column1.getWholeColumnName().split("\\.");
			String tableAlias1 = tableNameIndices[0];
			
			Column column2 = (Column)arg0.getRightExpression();
			tableNameIndices = column2.getWholeColumnName().split("\\.");
			String tableAlias2 = tableNameIndices[0];
			
			TablePair key = new TablePair(tableAlias1, tableAlias2);
			Expression value = joinConditions.get(key);
			if (value != null) {
				joinConditions.put(key, new AndExpression(value, 
					new MinorThan(arg0.getLeftExpression(), arg0.getRightExpression())));
			} else {
				joinConditions.put(key, 
					new MinorThan(arg0.getLeftExpression(), arg0.getRightExpression()));
			}
		} else {
		// In this case, it must have one side to be column and the other to be number
			Column column;
			if (arg0.getLeftExpression() instanceof Column) {
			    column = (Column)arg0.getLeftExpression();
			} else {
				column = (Column)arg0.getRightExpression();
			}
			String[] tableNameIndices = column.getWholeColumnName().split("\\.");
			String tableAlias = tableNameIndices[0];
			
			Expression value = scanConditions.get(tableAlias);
			if(value != null) {
				scanConditions.put(tableAlias, new AndExpression(value, 
					new MinorThan(arg0.getLeftExpression(), arg0.getRightExpression())));
			} else {
				scanConditions.put(tableAlias,
				new MinorThan(arg0.getLeftExpression(), arg0.getRightExpression()));
			}
		}		
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		// if left node and right node are both columns
		if ((arg0.getLeftExpression() instanceof Column) && 
				(arg0.getRightExpression() instanceof Column)) {
			Column column1 = (Column)arg0.getLeftExpression();
			String[] tableNameIndices = column1.getWholeColumnName().split("\\.");
			String tableAlias1 = tableNameIndices[0];
			
			Column column2 = (Column)arg0.getRightExpression();
			tableNameIndices = column2.getWholeColumnName().split("\\.");
			String tableAlias2 = tableNameIndices[0];
			
			TablePair key = new TablePair(tableAlias1, tableAlias2);
			Expression value = joinConditions.get(key);
			if (value != null) {
				joinConditions.put(key, new AndExpression(value, 
					new MinorThanEquals(arg0.getLeftExpression(), arg0.getRightExpression())));
			} else {
				joinConditions.put(key, 
					new MinorThanEquals(arg0.getLeftExpression(), arg0.getRightExpression()));
			}
		} else {
		// In this case, it must have one side to be column and the other to be number
			Column column;
			if (arg0.getLeftExpression() instanceof Column) {
			    column = (Column)arg0.getLeftExpression();
			} else {
				column = (Column)arg0.getRightExpression();
			}
			String[] tableNameIndices = column.getWholeColumnName().split("\\.");
			String tableAlias = tableNameIndices[0];
			
			Expression value = scanConditions.get(tableAlias);
			if(value != null) {
				scanConditions.put(tableAlias, new AndExpression(value, 
					new MinorThanEquals(arg0.getLeftExpression(), arg0.getRightExpression())));
			} else {
				scanConditions.put(tableAlias,
				new MinorThanEquals(arg0.getLeftExpression(), arg0.getRightExpression()));
			}
		}		
	}
	
	@Override
	public void visit(NotEqualsTo arg0) {
		// if left node and right node are both columns
		if ((arg0.getLeftExpression() instanceof Column) && 
				(arg0.getRightExpression() instanceof Column)) {
			Column column1 = (Column)arg0.getLeftExpression();
			String[] tableNameIndices = column1.getWholeColumnName().split("\\.");
			String tableAlias1 = tableNameIndices[0];
			
			Column column2 = (Column)arg0.getRightExpression();
			tableNameIndices = column2.getWholeColumnName().split("\\.");
			String tableAlias2 = tableNameIndices[0];
			
			TablePair key = new TablePair(tableAlias1, tableAlias2);
			Expression value = joinConditions.get(key);
			if (value != null) {
				joinConditions.put(key, new AndExpression(value, 
					new NotEqualsTo(arg0.getLeftExpression(), arg0.getRightExpression())));
			} else {
				joinConditions.put(key, 
					new NotEqualsTo(arg0.getLeftExpression(), arg0.getRightExpression()));
			}
		} else {
		// In this case, it must have one side to be column and the other to be number
			Column column;
			if (arg0.getLeftExpression() instanceof Column) {
			    column = (Column)arg0.getLeftExpression();
			} else {
				column = (Column)arg0.getRightExpression();
			}
			String[] tableNameIndices = column.getWholeColumnName().split("\\.");
			String tableAlias = tableNameIndices[0];
			
			Expression value = scanConditions.get(tableAlias);
			if(value != null) {
				scanConditions.put(tableAlias, new AndExpression(value, 
					new NotEqualsTo(arg0.getLeftExpression(), arg0.getRightExpression())));
			} else {
				scanConditions.put(tableAlias,
				new NotEqualsTo(arg0.getLeftExpression(), arg0.getRightExpression()));
			}
		}		
	}
	
	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Function arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub
		
	}

	

	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		
	}

	@Override
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Column arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub
		
	}
	

}
