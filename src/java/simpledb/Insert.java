package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private int tableid;
    private TransactionId tid;
    private TupleDesc returnTD;
    private boolean processed=false;
    
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
        // some code goes here

	this.child = child;
	this.tableid = tableid;
	this.tid = t;

	// verify that TupleDescriptors are the same
	if (!child.getTupleDesc().equals(
					 Database.getCatalog().getTupleDesc(tableid)))
	    throw new DbException("incompatible tuple descriptors for Insert");

	// we return a 1-field tuple
	Type[] typeAr = new Type[1];
	typeAr[0] = Type.INT_TYPE;
	returnTD = new TupleDesc(typeAr);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return returnTD;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
	child.open();
	super.open();
    }

    public void close() {
        // some code goes here
	super.close();
	child.close();
    }

    /**
     * You can just close and then open the child
     */
    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
	child.close();
	child.open();
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
        // some code goes here
        if (processed)
	    return null;

	int count = 0;
	while (child.hasNext()) {
	    Tuple t = child.next();
	    try {
		Database.getBufferPool().insertTuple(tid, tableid, t);
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
	    count++;
	}

	// finished scanning
	// generate a new "insert count" tuple
	Tuple tup = new Tuple(returnTD);
	tup.setField(0, new IntField(count));
	processed=true;
	return tup;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
	this.child = children[0];
    }
}
