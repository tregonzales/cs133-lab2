package simpledb;
import java.io.*;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    public TransactionId t_;

    public DbIterator child_;

    public int tableid_;

    public boolean hasFetched = false;

    public TupleDesc td_;
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
       t_ = t;
       child_ = child;
       tableid_ = tableid;
       Type[] typeArray = new Type[]{Type.INT_TYPE};
       String[] stringArray = new String[]{"inserted tuples"};
       td_ = new TupleDesc(typeArray, stringArray);

    }

    public TupleDesc getTupleDesc() {
        return td_;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
    }

    public void close() {
        super.close();
    }

    /**
     * You can just close and then open the child
     */
    public void rewind() throws DbException, TransactionAbortedException {
       child_.close();
       child_.open();
    }

    /**
     * Inserts tuples read from child into the relation with the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records (even if there are 0!). 
     * Insertions should be passed through BufferPool.insertTuple() with the 
     * TransactionId from the constructor. An instance of BufferPool is available via 
     * Database.getBufferPool(). Note that insert DOES NOT need to check to see if 
     * a particular tuple is a duplicate before inserting it.
     *
     * This operator should keep track if its fetchNext() has already been called, 
     * returning null if called multiple times.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
      int count;
       try{
           if (hasFetched == false){
            count = 0;
            child_.open();
            hasFetched = true;
            
            while(child_.hasNext()){
                Database.getBufferPool().insertTuple(t_, tableid_, child_.next());
                count++;
            }
            
            Tuple ourTuple = new Tuple(getTupleDesc());
            ourTuple.setField(0, new IntField(count));
            return ourTuple;
            }
       
       }

       catch(IOException e){

       } 
        
       return null;
        
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] {this.child_}; 
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (this.child_!=children[0]) {
            this.child_=children[0];
        }
    }
}
