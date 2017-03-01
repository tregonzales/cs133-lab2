package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    public DbIterator child_;

    public int afield_;

    public int gfield_;

    public Aggregator.Op aop_;

    public Tuple curChild;

    public boolean aggregated = false;

    public IntegerAggregator intAg;

    public StringAggregator stringAg;

    public DbIterator agIter;

    public TupleDesc td;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of fetchNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	   
        child_=child;
        afield_=afield;
        gfield_=gfield;
        aop_=aop;

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {

	   if(gfield_ == -1) {
            return simpledb.Aggregator.NO_GROUPING;
       }
	   else {
            return gfield_;
       }

    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {

	   return child_.getTupleDesc().getFieldName(gfield_);

    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {

	   return afield_;

    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {

	   return child_.getTupleDesc().getFieldName(afield_);

    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {

	   return aop_;

    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {

	   return aop.toString();

    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {

	   super.open();

    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     *
     * Hint: think about how many tuples you have to process from the child operator
     * before this method can return its first tuple.
     * Hint: notice that you each Aggregator class has an iterator() method
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        
        Type gbType;

        
        if(!aggregated) {
            open();
            child_.open();
            if(child_.hasNext()) {
                curChild=child_.next();
            }
            if(curChild.getField(afield_).getType().equals(Type.INT_TYPE)) {
                if(gfield_==-1){
                    intAg = new IntegerAggregator(gfield_, null ,afield_, aop_);
                    intAg.mergeTupleIntoGroup(curChild);
                }
                else{
                    gbType = curChild.getField(gfield_).getType();
                    intAg = new IntegerAggregator(gfield_, gbType ,afield_, aop_);
                    intAg.mergeTupleIntoGroup(curChild);
                }
                

                while(child_.hasNext()) {
                    intAg.mergeTupleIntoGroup(child_.next());
                }
               agIter = intAg.iterator();
               agIter.open();
            }
            else if (curChild.getField(afield_).getType().equals(Type.STRING_TYPE)) {
                     gbType = curChild.getField(gfield_).getType();
                     stringAg = new StringAggregator(gfield_, gbType ,afield_, aop_);
                     stringAg.mergeTupleIntoGroup(curChild);

                while(child_.hasNext()) {
                    stringAg.mergeTupleIntoGroup(child_.next());       
                }
                agIter = stringAg.iterator();
                agIter.open();
            }
            aggregated = true;
        }

        if(agIter.hasNext()) {
        return agIter.next();
        }
            
	       return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {

	       child_.rewind();

    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {

        
        int agIndex = afield_;

    	if (td == null) {

                Type[] agTypes;
                String[] agFields;
                
                if(gfield_ == -1) {
                    agIndex = 0;
                    agTypes = new Type[1];
                    agFields = new String[1];
                }
                else{
                    agTypes = new Type[2];
                    agFields = new String[2];
                }

                if(agTypes.length == 2) {
                    agIndex = 1;
                    agTypes[0] = curChild.getField(gfield_).getType();
                    agTypes[1] = Type.INT_TYPE;

                    agFields[0] = groupFieldName();
                    agFields[1] = aop_.toString();
                }
                else {
                    agTypes[0] = Type.INT_TYPE;
                    agFields[0] = aop_.toString();
                }

                    td = new TupleDesc(agTypes, agFields);      
            }

            return td;
        }

    public void close() {

	   super.close();

    }

    /**
     * See Operator.java for additional notes
     */
    @Override
    public DbIterator[] getChildren() {
	
        return new DbIterator[] {this.child_}; 

    }

    /**
     * See Operator.java for additional notes
     */
    @Override
    public void setChildren(DbIterator[] children) {
	   
        if (this.child_!=children[0]) {
            this.child_=children[0];
        }

    }
    
}
