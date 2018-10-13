cs5321 project 1

Team Member: Yixuan Jiang  (yj373) 
             Xiaoxing Yan  (xy363)
             Ruoxuan Xu   (rx65)
                        
1. Top level class for main method: src/App/Main

2. Extracting join conditions and build query plan:
	In this project, we use BasicVisitor to process all the conditions from WHERE and generate a query plan by sorting the expressions into join conditions and select-specific conditions. Here we explain with the example below.
	
	SELECT * 
	FROM Sailors AS S, Reserves AS R, Boats AS B 
	WHERE S.A = R.G AND R.G > 3
	
	- - When basicVisitor visit the PlainSelect object we get from JSqlParser, it will put all tables in PlainSelect.getFromItem() and PlainSelect.getJoins() into a list called tableDirectory: 
    [<S, Operator list of S>, <R, Operator list of R>, <B, Operator list of B>] 
    
    Each taleDirectory entry is a map from the table to the operators list added to the table, which curretly are the tables' relative ScanOperators.
    
     ScanOperator        ScanOperator              ScanOperator
     /                                       |                                    \
    S                                      R                                   B
    
	- - Then basicVisitor visits the expressions from getWhere() returned by JSqlParser, and the start point is the AND expression.
	                  AND
	             /              \
	          =                 > 
	         /  \               /   \
	   S.A     R.G   R.G    3
	 
	- - When basicVisitor visits AND, the child nodes of AND will accept the visit of basicVisitor, too; 
	
	- - BasicVisitor visit "=" in "S.A = R.G", and since its leftchild and rightchild are all column names, this expression is classified as join conditions. Correspondingly, the expression ">" in "R.G > 3" has leftchild as column and right child as number,  so it is classified as select-specifc conditions (which should be put directly at the parent node of its ScanOperator)
	
	 - - for join conditions "S.A = R.G", we create a joinOperator on top of S's current top Operator and R's current top Operator (if they are not the same), which joins the two tables involved in the join expression. Additionally, we also create a select operator on top of join Operator to select only the tuple we want.
    
                 SelectOperator
                     (S.A = R.G)
                            |
                            |
                  JoinOperator
               (join table S & R)
              /                         \
           /                               \
	    /                                     \                    
	 ScanOperator        ScanOperator              ScanOperator
     /                                       |                                    \
    S                                      R                                    B

	
	- -  for select-specific expression "R.G > 3", we create a selectOperator, which contains the expression,  right on top of the scan Operator.
	    
                   SelectOperator
                     (S.A = R.G)
                            |
                            |
                  JoinOperator
               (join table S & R)
              /                           \
            /                        SelectOperator
	      /                           ( R.G > 3)
	    /                                    |                               
	 ScanOperator        ScanOperator              ScanOperator
     /                                       |                                    \
    S                                      R                                   B

    
      - - After visiting the expression returned by getWhere(), we can do a final join to let all top operators joined together (if there are more than one top operators). 
                                     JoinOperator
                                    /                  \
                                  /                      \
                         SelectOperator          \
                     (S.A = R.G)                     \
                            |                                  \
                            |                                    \
                  JoinOperator                            \
               (join table S & R)                           \
              /                           \                            \
            /                        SelectOperator            \
	      /                           ( R.G > 3)                      \
	    /                                    |                                \    
	 ScanOperator        ScanOperator              ScanOperator
     /                                       |                                    \
    S                                      R                                   B

  - - finally, on top of the final join Operator, we add project operator and orderby operator.
  
                                   OrderByOperator
                                               |
                                    ProjectOperator
                                               |
                                     JoinOperator
                                    /                  \
                                  /                      \
                         SelectOperator          \
                     (S.A = R.G)                     \
                            |                                  \
                            |                                    \
                  JoinOperator                            \
               (join table S & R)                           \
              /                           \                            \
            /                        SelectOperator            \
	      /                           ( R.G > 3)                      \
	    /                                    |                                \    
	 ScanOperator        ScanOperator              ScanOperator
     /                                       |                                    \
    S                                      R                                   B

3. Use the query plan we generated in 2, generate tuples.

    To output a result table, we just call the getNextTuple() function on the root node of query plan OrderByOperator, then the OrderBy Operator will order all the tuples it get from its child Operator (by calling ProjectOperator.getNextTuple() ), and write to desinated output path;
    
    When ProjectOperator.getNextTuple() is called, ProjectOperator calls its child JoinOperator.getNextTuple() ... and the following cases are alike.
  