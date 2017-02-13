package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private int field1_;

    private Predicate.Op op_;

    private int field2_;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        field1_ = field1;
        op_ = op;
        field2_ = field2;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        Field fieldT1 = t1.getField(field1_);
        Field fieldT2 = t2.getField(field2_);
        Predicate.Op thisOperator = getOperator();

        return fieldT1.compare(thisOperator, fieldT2);


    }
    
    public int getField1()
    {
        return field1_;
    }
    
    public int getField2()
    {
        return field2_;
    }
    
    public Predicate.Op getOperator()
    {
        return op_;
    }
}
