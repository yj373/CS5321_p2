CS4321 project 2 (checkpoint 1)

Team Member: Yixuan Jiang  (yj373) 
                        Xiaoxing Yan  (xy363)
                        Ruoxuan Xu   (rx65)
                        
1. Top level class for main method: src/App/Main
    Logical query plan builder: src/App/LogicalPlanBuilder
    Physical query plan builder: src/Visitors/PhysicalPlanVisitor
    Tuple reader: src/util/TupleReader
    Tuple writer: src/uti/TupleWriter
    

2. Build the logical query plan
	There are two different phases in the process of building the logical query plan. First, the LogicalPlan Builder will visit all the fromItem and joinItems in the query. The builder will build the basic deep left tree structure of logical operators without expressions. In the second phase, the builder will use ExpressionClassifyVisitor to classify all the expressions. The expressions that should be assigned to scan operators will be stored in a map scanConditions, while the expressions that should be assigned to join operators will be stored in a map in joinConditions. Then, these two maps will be passed to LogicalPlanVisitor, and the LogicalPlanVisitor will visit the tree structure and assign expressions to the LogicalScanOperators and LogicalJoinOperators based on the two maps. All the logical operators are implemented and stored in the logicalOperators package.
    
3.Build the physical query plan
	After the logical query being built, the PhysicalPlanVisitor will visit the root logical operator and generate the physical query plan using visitor pattern. The logic of building the plan is introduced in the comments in the class PhysicalPlanVisitor.

4. Tuple reader and tuple writer
        To improve the perfomance of I/O, TupleReader and TupleWriter are implemented using NIO. The TupleReader will 
read in binary data in pages with the size of each page as 4096 bytes. Each scanOperator will create a TupleReader and every time the scanOperator calls getNextTuple, the TupleReader will call the method readNextTuple() to get the corresponding tuple. On the other hand, the TupleWriter will write tuples in pages with the same size as TupleReader. Everytime the physical operator calls dump(), a TupleWriter will be created and it will call the method writeTuple() which will write tuples in binary format and human readable format.


CS4321 project 2 (final milestone)

Team Member: Yixuan Jiang  (yj373) 
                        Xiaoxing Yan  (xy363)
                        Ruoxuan Xu   (rx65)
                        
In the final milestone, we maily implement BNLJ, SMJ and External Sort (uncomplete). And then, we create some test cases to compare the performances among 
BNLJ, SMJ and previous implemented TNLJ. Here is an overall introduction to our implementation.

1. BNLJ

Block nested loop join implementation:
Before joining tuples, read one block from outer relation tables and read one tuple from inner relation table. 
The implenmation follows the pseudocode:

Procedure BNLJ(outer R, inner S):

    for each block B of R do
        for each tuple s in S do
            for each tuple r in B do
                if r and s satisfy join condition then
                add new tuple formed from r and s to result


2. SMJ (in memory sort)

The Sort Merge Join Operator will firstly classify the columns in the join conditions, get the list of involved left columns on which its left table should be sorted, and the list its involved right columns on which its right table will be sorted. Then it will pass these "comparing standard" to its child sort operators, such that these sort operators can sort the tuples based on the list of columns.

In the class of Sort Merge Join Operator itself, it also has a customized comparator to compare the tuples from its left table and right table, which  is for multiple equality conditions on a single join operation. At the begining of the function, we use the comparator to determine which table has the smaller tuple, and then move the pointer of that table to the next entry to find possible match, keep track of right entry index, until the first match is found. At this time, we set pivot to be as the right tuple index, and begin the partition process. While left tuple is equal to right tuple, we move the right entry to the next and hold the left entry, until a mismatch is found. This is the case where we need to rechange the left tuple to its next entry, which reset the right index to its pivot value that we set when we first meet equation match (partition reset). Then we get back to the very first situation that we have discussed in the beginning.

To  handle the reset of the right entry during partition process, we call the reset function of Tuple Reader in its child sort operator, such that the tuple reader will compute the page number, file position and buffer position to reload the channel and buffer to get the tuple with the right index to Sort-Merge-Join Operator. In such way, we avoid storing all these tuple variables in memory and  unbounded states.

DISTINCT: we implemented using a sorting approach mentioned in Project 1, therefore, it does not use unbounded state.

3. External Sort

    The implementation of External sort corresponds to the class in src/operators/ExternalSortOperators. We need to give in total five parameters to construct a 
    ExternalSortOperator. The first parameter is the current query number which will be used to clear files in the temp directory in the future. The second parameter 
    is the size of the buffer that is used to sort tuples. In our case, the buffer is an array of Tuples with fixed size (determined by the the user defined buffer size and 
    the size od each tuple). The third parameter is a list of columns which determines the order of sorting. The forth parameter is the schema of this operator and 
    the child operator is given by the fifth parameter. 
    
    As for the B-1 way sorting algorithm, there are several passes. In the pass 0, the ExternalSortOperator simply continuously read in tuples from its dhild operator 
    until the sorting buffer is full. If the reading is terminated because the buffer is full, the read phase will return a state flag '1'; if the reading is terminated because 
    there is no more tuple that can be read from the child operator, the read phase will return a state flag '0'.During the pass 0, the sort buffer will sort the tuples in 
    memory, and then write out the sorting results in several files which is named in a format as following "aliase_attributes_passNum_fileNum_queryNum". These 
    files will be put in a corresponding subdirectory 'temp/external-sort/nameOfExternalSort'. After the pass 0,  the ExternalSortOperator will create at most B-1 
    tuple readers, to merge at most B-1 sorted files in last pass. Then, the ExternalSortOperator will read tuples from those tuple readers. If the reading is 
    terminated because the buffer is full, the reading phase will return a state flag '3'; if the reading is terminated because there is no more tuple that can be read 
    from the child operator, the reading phase will return a state flag '2'. Once the sort buffer is full, the operator will execute a mergesort() method which will merge 
    the currently sorted B-1 tuple arrays and write them into a file. And the merge process will continue untill all the tuple readers stop returning tuples, which
    means the a state flag '2' is returned by the reading phase. After the merging is finished, the corresponding files of last pass (files that read by the tuple readers)
    will be teleted. This iteration will continue until only two files left in the subdirectory (the sorted file and its corresponding human readable file). At last, the 
    operator will create a tuple reader to read the final sorted file. When the getnextTuple() method is called, this tuple reader will read a tuple from the sorted file.
    When the reset() method is called, we simply reset this tuple reader.


4. Known bugs

    There are still some bugs in our implementation of ExternalSortOperator. If we used ExternalSortOperator instead of SortOperator in the query plan, several 
    tuples will be ignored by the operator, which means the output tuples are less than the expected results. We have spent much time on solving this problem, but 
    at least by the deadline, we are not able to solve it. We think this bug may relate to our merge sorting algorithm, and to be more specific, this bug can be 
    caused by wrongly updating the sorting buffer pointers. In the next few days, we will continue fixing this bug.



