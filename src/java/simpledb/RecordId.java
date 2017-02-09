package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    //instance variable
    public PageId pageId;

    //instance variable
    public int numTuple;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        pageId = pid;
        numTuple = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        
        return numTuple;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        
        return pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        
        RecordId r = (RecordId)o;
        if(numTuple == r.numTuple && pageId.equals(r.pageId)) {
            return true;
        }
        else {
            return false;
        }

    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {

        //the gonzales-mccarthy method
        return 32768*numTuple;

    }

}
