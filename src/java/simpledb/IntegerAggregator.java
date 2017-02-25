package simpledb;
import java.util.*;
/**
 * Computes some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    public int gbfield_;

    public Type gbfieldtype_;

    public int afield_;

    public Op what_;

    public Map<Field, Tuple> agTups = new HashMap<Field, Tuple>();

    public Map<Field, Integer> sums = new HashMap<Field, Integer>();

    public Map<Field, Integer> counts = new HashMap<Field, Integer>();

    public Map<Field, Integer> mins = new HashMap<Field, Integer>();

    public Map<Field, Integer> maxs = new HashMap<Field, Integer>();

    public TupleDesc td;

    public int agIndex;

    public Field gbFieldKey;


    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        gbfield_ = gbfield;
        gbfieldtype_ = gbfieldtype;
        afield_=afield;
        what_=what;

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor. See Aggregator.java for more.
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {


       
        gbFieldKey = tup.getField(gbfield_);

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
            agTups.put(gbFieldKey, new Tuple(td));
            sums.put(gbFieldKey, 0);
            counts.put(gbFieldKey, 0);
            mins.put(gbFieldKey, null);
            maxs.put(gbFieldKey, null);
        }

        if(what_.equals(Aggregator.Op.SUM)) {

            int dis1 = ((IntField)tup.getField(afield_)).getValue() + sums.get(gbFieldKey);
            sums.put(gbFieldKey, (Integer)dis1);
            IntField dis1if = new IntField(dis1);

            Tuple disTup;

            if(gbfield_ == -1) {
                disTup = agTups.get(gbFieldKey);
                disTup.setField(agIndex, dis1if);

            }
            else{
                disTup = agTups.get(gbFieldKey);  
                disTup.setField(0, gbFieldKey);   
                disTup.setField(agIndex, dis1if);
            }

            agTups.put(gbFieldKey, disTup);

        }
        else if(what_.equals(Aggregator.Op.AVG)) {

            int dis2 = ((IntField)tup.getField(afield_)).getValue() + sums.get(gbFieldKey); //may need to cast this because returns integer type, not int
            sums.put(gbFieldKey, (Integer)dis2);

            int newCount2 = counts.get(gbFieldKey) + 1;
            counts.put(gbFieldKey, (Integer)newCount2);

            int avg = dis2/newCount2;
            IntField avgif = new IntField(avg);

            Tuple disTup;

            if(gbfield_ == -1) {
                disTup = agTups.get(gbFieldKey);
                disTup.setField(agIndex, avgif);

            }
            else{
                disTup = agTups.get(gbFieldKey);  
                disTup.setField(0, gbFieldKey);   
                disTup.setField(agIndex, avgif);
            }
            

            agTups.put(gbFieldKey, disTup);

        }
        else if(what_.equals(Aggregator.Op.COUNT)) {

            int newCount = counts.get(gbFieldKey) + 1;
            counts.put(gbFieldKey, (Integer)newCount);

            IntField newCountif = new IntField(newCount);

            Tuple disTup;

            if(gbfield_ == -1) {
                disTup = agTups.get(gbFieldKey);
                disTup.setField(agIndex, newCountif);

            }
            else{
                disTup = agTups.get(gbFieldKey);  
                disTup.setField(0, gbFieldKey);   
                disTup.setField(agIndex, newCountif);
            }

            

            agTups.put(gbFieldKey, disTup);

        }
        else if(what_.equals(Aggregator.Op.MIN)) {

            Tuple disTup;

           
            if(mins.get(gbFieldKey) == null) {
                int tupVal = ((IntField)tup.getField(afield_)).getValue();
                IntField datif = new IntField(tupVal);

                if(gbfield_ == -1) {
                    disTup = agTups.get(gbFieldKey);
                    disTup.setField(agIndex, datif);

            }
                else{
                    disTup = agTups.get(gbFieldKey);  
                    disTup.setField(0, gbFieldKey);   
                    disTup.setField(agIndex, datif);
            }
                mins.put(gbFieldKey, tupVal);
                agTups.put(gbFieldKey, disTup);

            }
            else {
            int tupVal = ((IntField)tup.getField(afield_)).getValue();
            int minVal = mins.get(gbFieldKey);

            if(tupVal < minVal) {
            mins.put(gbFieldKey, tupVal);

            IntField datif = new IntField(tupVal);

            

            if(gbfield_ == -1) {
                disTup = agTups.get(gbFieldKey);
                disTup.setField(agIndex, datif);

            }
            else{
                disTup = agTups.get(gbFieldKey);  
                disTup.setField(0, gbFieldKey);   
                disTup.setField(agIndex, datif);
            }
            
            agTups.put(gbFieldKey, disTup);

            }
        }

        

    }
        else if(what_.equals(Aggregator.Op.MAX)) {

            Tuple disTup;
           
            if(maxs.get(gbFieldKey) == null) {
                int tupVal = ((IntField)tup.getField(afield_)).getValue();
                IntField datif = new IntField(tupVal);

                if(gbfield_ == -1) {
                    disTup = agTups.get(gbFieldKey);
                    disTup.setField(agIndex, datif);

            }
                else{
                    disTup = agTups.get(gbFieldKey);  
                    disTup.setField(0, gbFieldKey);   
                    disTup.setField(agIndex, datif);
            }
                maxs.put(gbFieldKey, tupVal);
                agTups.put(gbFieldKey, disTup);

            }
            else {
            int tupVal = ((IntField)tup.getField(afield_)).getValue();
            int maxVal = maxs.get(gbFieldKey);

            if(tupVal > maxVal) {
            maxs.put(gbFieldKey, tupVal);

            IntField datif = new IntField(tupVal);

            

            if(gbfield_ == -1) {
                disTup = agTups.get(gbFieldKey);
                disTup.setField(agIndex, datif);

            }
            else{
                disTup = agTups.get(gbFieldKey);  
                disTup.setField(0, gbFieldKey);   
                disTup.setField(agIndex, datif);
            }
            
            agTups.put(gbFieldKey, disTup);

            }
        }

        

    }
}

    /**
     * Returns a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {

         TupleIterator plz = new TupleIterator(td, agTups.values());

        return plz;
        
    }

}
