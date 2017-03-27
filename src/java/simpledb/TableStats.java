package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }


    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() throws IOException, DbException, TransactionAbortedException {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    public int numPages;
    public int cost;
    public Map<Integer, IntHistogram> ih; 
    public Map<Integer, StringHistogram> sh;
    public int numTuples=0;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) throws IOException, DbException, TransactionAbortedException {

        try {
       
        cost=ioCostPerPage;
        DbFile d = Database.getCatalog().getDatabaseFile(tableid);
        HeapFile f = (HeapFile)d;
        TupleDesc td = f.getTupleDesc();
        numPages = f.numPages();
        int numFields = td.numFields();

        //use field number in td as key, keeps track of mins and maxes and string values
        Map<Integer, Integer> mins = new HashMap<Integer, Integer>();
        Map<Integer, Integer> maxs = new HashMap<Integer, Integer>();

        ih = new  HashMap<Integer, IntHistogram>();
        sh = new  HashMap<Integer, StringHistogram>();

        //can create a heapFile with a file and a tupledesc
        //HeapFile: numPages()

        //most of the work done in this snippet
        Transaction t = new Transaction(); 
        t.start(); 
        SeqScan s = new SeqScan(t.getId(), tableid, "t"); 
        Tuple curTup;
        s.open();

        while(s.hasNext()) {

            curTup = s.next();
            numTuples++;

            for(int i=0; i<numFields; i++) {

                if(curTup.getField(i).getType().equals(Type.INT_TYPE)) {
                    //check if key is there, if not place new min
                    if(!mins.containsKey(i)) {
                        mins.put(i, (((IntField)(curTup.getField(i)))).getValue());
                    }
                    else if(((IntField)(curTup.getField(i))).getValue() < mins.get(i)) {
                        mins.put(i, ((IntField)(curTup.getField(i))).getValue());
                    }
                    //chck same thing for max
                    if(!maxs.containsKey(i)) {
                        maxs.put(i, ((IntField)(curTup.getField(i))).getValue());
                    }
                    else if(((IntField)(curTup.getField(i))).getValue() > maxs.get(i)) {
                        maxs.put(i, ((IntField)(curTup.getField(i))).getValue());
                    }
                }
            }
        }

        s.rewind();

            while(s.hasNext()) {

                curTup = s.next();

                for(int i=0; i<numFields; i++) {
                    if(td.getFieldType(i).equals(Type.INT_TYPE)) {

                        if(!ih.containsKey(i)) {
                            ih.put(i, new IntHistogram(NUM_HIST_BINS, mins.get(i), maxs.get(i)));
                            ih.get(i).addValue(((IntField)(curTup.getField(i))).getValue());
                        }
                        else {
                            ih.get(i).addValue(((IntField)(curTup.getField(i))).getValue());
                        }
                    }
                    else {
                        if(!sh.containsKey(i)) {
                            sh.put(i, new StringHistogram(NUM_HIST_BINS));
                            sh.get(i).addValue(((StringField)(curTup.getField(i))).getValue());
                        }
                        else {
                            sh.get(i).addValue(((StringField)(curTup.getField(i))).getValue());
                        }
                    }
                }
            }
        t.commit();
    }
    catch (DbException ex) {
        throw ex;
    }
    catch (TransactionAbortedException ex) {
        throw ex;

     }
     catch (IOException ex) {
         throw ex;
     }


        

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {

        return (double)(numPages*cost);
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)(selectivityFactor*numTuples);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     *
     * Not necessary for lab 3
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        return 0.5;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        
        if(constant.getType().equals(Type.INT_TYPE)) {
            return ih.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
        }
        else {
            return sh.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
        }
      
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return numTuples;
    }

}
