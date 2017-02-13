package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate p_;

    public DbIterator child_;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        p_ = p;
        child_ = child;
    }

    public Predicate getPredicate() {
        return p_;
    }

    public TupleDesc getTupleDesc() {
        return child_.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child_.open();
        super.open();
    }

    public void close() {
        super.close();
        child_.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child_.rewind();
    }

    /**
     * The Filter operator iterates through the tuples from its child, 
     * applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * This method returns the next tuple.
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
       
       child_.open();

       while (child_.hasNext()){
           Tuple nextTuple = child_.next();
           if (getPredicate().filter(nextTuple)){
               return nextTuple;
           }
                
       }

       child_.close();
       return null;
        
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
        if (this.child_!=children[0]){
            this.child_=children[0];
        }
    }

}
