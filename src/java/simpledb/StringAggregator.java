package simpledb;
import java.util.*;

/**
 * Computes some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    public int gbfield_;

    public Type gbfieldtype_;

    public int afield_;

    public Op what_;

    public Map<Field, Tuple> agTups = new HashMap<Field, Tuple>();

    //public Map<Field, Integer> counts = new HashMap<Field, Integer>();

    public TupleDesc td;

    public int agIndex;

    public Field gbFieldKey;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        gbfield_ = gbfield;
        gbfieldtype_ = gbfieldtype;
        afield_=afield;
        what_=what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {

        gbFieldKey = tup.getField(gbfield_);
        Tuple dis;

        if (td == null) {

            Type[] agTypes;
            String[] agFields;
            
            if(gbfield_ == -1) {
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
                agTypes[0] = gbfieldtype_;
                agTypes[1] = Type.INT_TYPE;

                agFields[0] = gbFieldKey.toString();
                agFields[1] = what_.toString();
            }
            else {
                agTypes[0] = Type.INT_TYPE;
                agFields[0] = what_.toString();
            }

                td = new TupleDesc(agTypes, agFields);
                
        }

        if(!agTups.containsKey(gbFieldKey)) {

            if(gbfield_ == -1) {
                dis = new Tuple(td);
                IntField dat = new IntField(1);
                dis.setField(agIndex, dat);
                agTups.put(gbFieldKey, dis);
            }
            else{
                dis = new Tuple(td);
                IntField dat = new IntField(1);
                dis.setField(0, gbFieldKey);
                dis.setField(agIndex, dat);
                agTups.put(gbFieldKey, dis);
            }

        }
        else{
            Tuple dis1 = agTups.get(gbFieldKey);
            int a = ((IntField)agTups.get(gbFieldKey).getField(agIndex)).getValue();
            IntField dat1 = new IntField(a+1);
            dis1.setField(agIndex, dat1);
            agTups.put(gbFieldKey, dis1);
        }


    }


    /**
     * Returns a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        
        // Iterator<Tuple> ti = agTups.values().iterator();
        // Iterable<Tuple> tat = ti;
        TupleIterator plz = new TupleIterator(td, agTups.values());

        return plz;
    }

}
