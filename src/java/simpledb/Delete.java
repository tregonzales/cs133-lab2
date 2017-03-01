package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    public TransactionId t_;

    public DbIterator child_;

    public TupleDesc td_;

    public boolean hasFetched = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        t_ = t;
        child_ = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method. You can pass along the TransactionId from the constructor.
     * This operator should keep track of whether its fetchNext() method has been called already. 
     * 
     * @return A 1-field tuple containing the number of deleted records (even if there are 0)
     *          or null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
       boolean treThing = true;
       int count = 0;
       try{
           if (hasFetched == false){
            //count = 0;
            child_.open();
            hasFetched = true;
            
            while(child_.hasNext()){
                treThing = true;
                try{
                    Database.getBufferPool().deleteTuple(t_,child_.next());
                }
                catch(DbException e){
                    treThing = false;
                }
                if (treThing == true){
                     count++;
                }
               
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
