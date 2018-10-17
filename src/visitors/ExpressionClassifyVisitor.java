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

/**
 * The class of ExpressionClassifierVisitor is a visitor to visit query expressions parsed
 * by JSqlParser, and classify all these conditions as join conditions and scan conditions.
 * Join conditions should be examined by JoinOperator, while Scan conditions should be examined
 * by the ScanOperator. The query expressions include several types below:
 * 
 *  1. <sub expression> AND <sub expression>
 *  2. S.A = 10 (>=, <=, >, <)
 *  3. Sailors.A = Sailors.B (>=, <=, >, <)
 *  4. Sailors.A = Reserves.D (>=, <=, >, <)
 * 
 * For the 1st case, ExpressionClassifierVisitor will visit sub expression respectively.
 * 
 * For the 2nd case and the 3rd case, it should be classified as scan conditions, which can be
 * examined directly when a tuple is loaded into the ScanOperator.
 * 
 * For the 3rd case, it should be classified as join conditions, which can only be checked in 
 * the JoinOperator which includes both tables of Sailors and Reserves.
 * 
 * This class puts all the scan conditions into a Map <TableAlias, Expression>, as every expression
 * only relates to one table; all the joinConditions are put into a Map <TablePair, Expression>, 
 * as all the join conditions involve two tables as a pair.
 * 
 * @author Ruoxuan Xu
 *
 */

public class ExpressionClassifyVisitor implements ExpressionVisitor{
	
	/** the map to store all joinCondtions extracted from JSqlParser */
	private Map<TablePair, Expression> joinConditions = new HashMap<>();
	
	/** the map to store all scanCondtions extracted from JSqlParser */
	private Map<String, Expression> scanConditions = new HashMap<>();
	
	/**
	 * get the map between all table aliases and their relative scan conditions.
	 * @return scan conditions
	 */
	public Map<String, Expression> getScanConditions() {
		return scanConditions;
	}
	
	/**
	 * get the map between all table pairs and their respective join conditions.
	 * @return join conditions
	 */
	public Map<TablePair, Expression> getJoinConditions() {
		return joinConditions;
	}
	
	/**
	 * The start point of this function: visit the root of Expression parsed by 
	 * JSqlParser.
	 * @param ps: the directed output from JSalParser.
	 */
	public void classify(PlainSelect ps) {
		if (ps != null) {
			Expression origin = ps.getWhere();
		    if (origin != null) {
		    	origin.accept(this);
		    }
		}
	}

	/** visit the EqualsTo expression
	 *  @param arg0: the EqualsTo expression
	 */
	@Override
	public void visit(EqualsTo arg0) {
		/* if left node and right node are both columns */
		if ((arg0.getLeftExpression() instanceof Column) && 
				(arg0.getRightExpression() instanceof Column)) {
			Column column1 = (Column)arg0.getLeftExpression();
			String[] tableNameIndices = column1.getWholeColumnName().split("\\.");
			String tableAlias1 = tableNameIndices[0];
			
			Column column2 = (Column)arg0.getRightExpression();
			tableNameIndices = column2.getWholeColumnName().split("\\.");
			String tableAlias2 = tableNameIndices[0];
			
			/* corner case: if the two columns are for the same table, it goes to scan conditions*/
			if (tableAlias1.equals(tableAlias2)) {
				Expression prevCondi = scanConditions.get(tableAlias1);
				if (prevCondi == null) {
					scanConditions.put(tableAlias1, 
							new EqualsTo(arg0.getLeftExpression(), arg0.getRightExpression()));
				} else {
					scanConditions.put(tableAlias1,new AndExpression(prevCondi,
							new EqualsTo(arg0.getLeftExpression(), arg0.getRightExpression())));
				}
			} else {
		    /* else the two columns are of different table, thus it should be put into join conditions */
				TablePair key = new TablePair(tableAlias1, tableAlias2);
				Expression value = joinConditions.get(key);
				if (value != null) {
					joinConditions.put(key, new AndExpression(value, 
						new EqualsTo(arg0.getLeftExpression(), arg0.getRightExpression())));
				} else {
					joinConditions.put(key, 
						new EqualsTo(arg0.getLeftExpression(), arg0.getRightExpression()));
				}
			}
			
		} else {
		/* In this case, it must have one side to be column and the other to be number */
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
	
	
	/** visit the GreaterThan expression
	 *  @param arg0: the GreaterThan expression
	 */
	@Override
	public void visit(GreaterThan arg0) {
		/* if left node and right node are both columns */
		if ((arg0.getLeftExpression() instanceof Column) && 
				(arg0.getRightExpression() instanceof Column)) {
			Column column1 = (Column)arg0.getLeftExpression();
			String[] tableNameIndices = column1.getWholeColumnName().split("\\.");
			String tableAlias1 = tableNameIndices[0];
			
			Column column2 = (Column)arg0.getRightExpression();
			tableNameIndices = column2.getWholeColumnName().split("\\.");
			String tableAlias2 = tableNameIndices[0];
			/* corner case: if the two columns are for the same table, it goes to scan conditions*/
			if (tableAlias1.equals(tableAlias2)) {
				Expression prevCondi = scanConditions.get(tableAlias1);
				if (prevCondi == null) {
					scanConditions.put(tableAlias1, 
							new GreaterThan(arg0.getLeftExpression(), arg0.getRightExpression()));
				} else {
					scanConditions.put(tableAlias1,new AndExpression(prevCondi,
							new GreaterThan(arg0.getLeftExpression(), arg0.getRightExpression())));
				}
			} else {
			/* else the two columns are of different table, thus it should be put into join conditions */
				TablePair key = new TablePair(tableAlias1, tableAlias2);
				Expression value = joinConditions.get(key);
				if (value != null) {
					joinConditions.put(key, new AndExpression(value, 
						new GreaterThan(arg0.getLeftExpression(), arg0.getRightExpression())));
				} else {
					joinConditions.put(key, 
						new GreaterThan(arg0.getLeftExpression(), arg0.getRightExpression()));
				}
			}
		} else {
		/* In this case, it must have one side to be column and the other to be number */
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

	/** visit the GreaterThanEquals expression
	 *  @param arg0: the GreaterThanEquas expression
	 */
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
			
			if (tableAlias1.equals(tableAlias2)) {
				Expression prevCondi = scanConditions.get(tableAlias1);
				if (prevCondi == null) {
					scanConditions.put(tableAlias1, 
							new GreaterThanEquals(arg0.getLeftExpression(), arg0.getRightExpression()));
				} else {
					scanConditions.put(tableAlias1,new AndExpression(prevCondi,
							new GreaterThanEquals(arg0.getLeftExpression(), arg0.getRightExpression())));
				}
			} else {		
				TablePair key = new TablePair(tableAlias1, tableAlias2);
				Expression value = joinConditions.get(key);
				if (value != null) {
					joinConditions.put(key, new AndExpression(value, 
						new GreaterThanEquals(arg0.getLeftExpression(), arg0.getRightExpression())));
				} else {
					joinConditions.put(key, 
						new GreaterThanEquals(arg0.getLeftExpression(), arg0.getRightExpression()));
				}
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
	
    /** visit the MinorThan expression
	 *  @param arg0: the MinorThan expression
	 */
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
			
			if (tableAlias1.equals(tableAlias2)) {
				Expression prevCondi = scanConditions.get(tableAlias1);
				if (prevCondi == null) {
					scanConditions.put(tableAlias1, 
							new MinorThan(arg0.getLeftExpression(), arg0.getRightExpression()));
				} else {
					scanConditions.put(tableAlias1,new AndExpression(prevCondi,
							new MinorThan(arg0.getLeftExpression(), arg0.getRightExpression())));
				}
			} else {
				TablePair key = new TablePair(tableAlias1, tableAlias2);
				Expression value = joinConditions.get(key);
				if (value != null) {
					joinConditions.put(key, new AndExpression(value, 
						new MinorThan(arg0.getLeftExpression(), arg0.getRightExpression())));
				} else {
					joinConditions.put(key, 
						new MinorThan(arg0.getLeftExpression(), arg0.getRightExpression()));
				}
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

	/** visit the MinorThanEquals expression
	 *  @param arg0: the MinorThanEquals expression
	 */
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
			
			if (tableAlias1.equals(tableAlias2)) {
				Expression prevCondi = scanConditions.get(tableAlias1);
				if (prevCondi == null) {
					scanConditions.put(tableAlias1, 
							new MinorThanEquals(arg0.getLeftExpression(), arg0.getRightExpression()));
				} else {
					scanConditions.put(tableAlias1,new AndExpression(prevCondi,
							new MinorThanEquals(arg0.getLeftExpression(), arg0.getRightExpression())));
				}
			} else {		
				TablePair key = new TablePair(tableAlias1, tableAlias2);
				Expression value = joinConditions.get(key);
				if (value != null) {
					joinConditions.put(key, new AndExpression(value, 
						new MinorThanEquals(arg0.getLeftExpression(), arg0.getRightExpression())));
				} else {
					joinConditions.put(key, 
						new MinorThanEquals(arg0.getLeftExpression(), arg0.getRightExpression()));
				}
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
	
	/** visit the NotEqualsTo expression
	 *  @param arg0: the NotEqualsTo expression
	 */
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
			
			if (tableAlias1.equals(tableAlias2)) {
				Expression prevCondi = scanConditions.get(tableAlias1);
				if (prevCondi == null) {
					scanConditions.put(tableAlias1, 
							new NotEqualsTo(arg0.getLeftExpression(), arg0.getRightExpression()));
				} else {
					scanConditions.put(tableAlias1,new AndExpression(prevCondi,
							new NotEqualsTo(arg0.getLeftExpression(), arg0.getRightExpression())));
				}
			} else {
				TablePair key = new TablePair(tableAlias1, tableAlias2);
				Expression value = joinConditions.get(key);
				if (value != null) {
					joinConditions.put(key, new AndExpression(value, 
						new NotEqualsTo(arg0.getLeftExpression(), arg0.getRightExpression())));
				} else {
					joinConditions.put(key, 
						new NotEqualsTo(arg0.getLeftExpression(), arg0.getRightExpression()));
				}
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
