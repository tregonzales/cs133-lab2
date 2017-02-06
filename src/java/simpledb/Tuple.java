package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /* instance variable for contructor of Tuple 
    */
    public TupleDesc tupleDesc; 

    /* instance variable for constructor of tuple, array of fields and values
    */
    public Field tupleList[];

    //so basically the tuple has a tupledesc containing the schema, and then it has a list of
    //fields that contain the values

    public RecordId rid;


    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.tupleDesc = td;
        this.tupleList = new Field[td.tdList.length];
        
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
     
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid=rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        
        tupleList[i] = f;

    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        
        return tupleList[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        String columnString="";

        for(int i=0; i<tupleList.length-1; i++) {
            columnString+= tupleList[i].toString() + "\t";
        }
        columnString+= tupleList[tupleList.length-1]+"\n";

        return columnString;

        //throw new UnsupportedOperationException("Implement this");
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return Arrays.asList(tupleList).iterator();
    }
    
    /**
     * Reset the TupleDesc of this tuple
     * Does not need to worry about the fields inside the Tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this.tupleDesc = td; 
    }
}
