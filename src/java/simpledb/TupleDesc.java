package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        List<TDItem> myList =  Arrays.asList();
        return myList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /* instance variable for TupleDesc constructor
    */
    public TDItem tdList[];

    /* field length count of TupleDesc
    */
    public int numFields;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        this.tdList = new TDItem[typeAr.length];
        this.numFields = 0;

        for (int i = 0; i < typeAr.length; i++) {

            if (fieldAr[i].equals(null)) {
               tdList[i] = new TDItem(typeAr[i], "");
               numFields++;
            }
            else {
                tdList[i] = new TDItem(typeAr[i], fieldAr[i]);
                numFields++;
            }
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this.tdList = new TDItem[typeAr.length];

        for (int i = 0; i < typeAr.length; i++) {

               tdList[i] = new TDItem(typeAr[i], null);
               numFields++;
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        
        return numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {

        if (i >= tdList.length)
        {
            throw new NoSuchElementException();
        }
        else{
              return tdList[i].fieldName;
        }
      
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        
        return tdList[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        
        
        if(name == null) {
            throw new NoSuchElementException();
        }
            else{
                for (int i = 0; i < tdList.length; i++){
                    if (name.equals(tdList[i].fieldName)){
                        return i;
                        }
                    }
                }
        throw new NoSuchElementException();
    }
    

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
       int size = 0;
       for(int i=0; i<tdList.length; i++) {
        
        size+=tdList[i].fieldType.getLen();
       }
       return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        TupleDesc merged = new TupleDesc(new Type[0]);
        merged.tdList = new TDItem[td1.tdList.length + td2.tdList.length];
        for(int i=0; i<td1.tdList.length; i++) {
            merged.tdList[i] = td1.tdList[i];
            }

        for(int i=0; i<td2.tdList.length; i++) {
            merged.tdList[td1.tdList.length+i] = td2.tdList[i];
            }

        merged.numFields = merged.tdList.length;

            return merged;
        }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc)){
           return false; 
        }
        else{
            TupleDesc e = (TupleDesc)o;
            if (e.tdList.length != tdList.length) { 
                return false;
            }
            else {
                for(int i = 0; i<tdList.length; i++) {
                    if (!(tdList[i].fieldType.equals(e.tdList[i].fieldType))) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String tdString="";

        for(int i=0; i<tdList.length-1; i++) {
            tdString = tdString + tdList[i].fieldName.toString() + "[" + i + "]("
            + tdList[i].fieldType.toString() + "), ";
        }
        tdString = tdString + tdList[tdList.length-1].fieldName.toString() + "[" + 
        ((tdList.length)-1) + "]("+tdList[tdList.length-1].fieldType.toString() + ")";

        return tdString;
        
    }
}
