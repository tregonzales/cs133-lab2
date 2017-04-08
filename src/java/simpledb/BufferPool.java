package simpledb;

import java.io.*;
import java.util.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    final int numPages;
    final ConcurrentHashMap<PageId,Page> pages; // hash table storing current pages in memory
    private final Random random = new Random(); // for choosing random pages for eviction

    /* For Lab 4: instance of a private Lock Manager class. 
       Should be instantiated in the constructor for BufferPool. */
    private final LockManager lockmgr; // Added for Lab 4

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
    
    this.numPages = numPages;
    this.pages = new ConcurrentHashMap<PageId, Page>();
    
    lockmgr = new LockManager(); // Added for Lab 4
    }
    
    public static int getPageSize() {
    return pageSize;
    }
    
    /**
     * Helper: this should be used for testing only!!!
     */
    public static void setPageSize(int pageSize) {
    BufferPool.pageSize = pageSize;
    }
    
    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
    throws TransactionAbortedException, DbException {
    
        
    try {   // Added for Lab 4: acquire the lock on the page first
        lockmgr.acquireLock(tid, pid, perm);
    } catch (DeadlockException e) { 
        throw new TransactionAbortedException(); // caught by callee, who calls transactionComplete()
    }
    
    Page p;
    synchronized(this) {
        p = pages.get(pid);
        if(p == null) {
        if(pages.size() >= numPages) {
            evictPage();// added for lab 2
            // throw new DbException("Out of buffer pages");
        }
        
        p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        pages.put(pid, p);
        }
    }
    return p;
    }

    /**
     * Releases the lock on a page. Mainly used for testing.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
    // not necessary for lab1|lab2
    lockmgr.releaseLock(tid,pid); // Added for Lab 4
    }
    
    /**
     * Release all locks associated with a given transaction.
     * Calls other version of transactionComplete() with commit set to true.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException { 
    // not necessary for lab1|lab2
    transactionComplete(tid,true); // Added for Lab 4
    }
    
    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
    // not necessary for lab1|lab2
    return lockmgr.holdsLock(tid, p); // Added for Lab 4
    }
    
    /**
     * Commit or abort a given transaction; then release all locks associated to
     * the transaction.
     * If commit == true, be sure to flush any dirty pages from this xact
     * If commit == false, this signifies an abort, so any changes to dirty 
     * pages should be thrown out of the buffer pool.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
    throws IOException {
    
    if(commit){
        ArrayList<PageId> dis = lockmgr.getTidLocks().get(tid);
        for(PageId p: dis){
            flushPage(p);
        }
    }
    else{
        ArrayList<PageId> dis = lockmgr.getTidLocks().get(tid);
        for(PageId p: dis){
            pages.remove(p);
        }
    }
    
    
    // after dealing with commit vs. abort actions, ask lock manager to release locks
    lockmgr.releaseAllLocks(tid); // Added for Lab 4
    }
    
    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
    throws DbException, IOException, TransactionAbortedException {
    // some code goes here
    // not necessary for lab1
    
    DbFile file = Database.getCatalog().getDatabaseFile(tableId);
    
    // let the specific implementation of the file decide which page to add it to
    ArrayList<Page> dirtypages = file.insertTuple(tid, t);
    
    synchronized(this) {
        for (Page p : dirtypages){
        p.markDirty(true, tid);
        
        // if page in pool already, done.
        if(pages.get(p.getId()) != null) {
            //replace old page with new one in case insertTuple returns a new copy of the page
            pages.put(p.getId(), p);
        }
        else {
            // put page in pool
            if(pages.size() >= numPages)
            evictPage();
            pages.put(p.getId(), p);
        }
        }
    }
    }
    
    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
    throws DbException, IOException, TransactionAbortedException {
    // some code goes here
    // not necessary for lab1
    
    DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
    ArrayList<Page> dirtypages = file.deleteTuple(tid, t);
    
    synchronized(this) {
        for (Page p : dirtypages){
        p.markDirty(true, tid);
        }
    }
    }
    
    /**
     * Flush all dirty pages to disk.
     * Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    // some code goes here
    // not necessary for lab1
    
    Iterator<PageId> i = pages.keySet().iterator();
    while(i.hasNext())
        flushPage(i.next());
    
    }
    
    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
    // some code goes here
    // not necessary for labs 1--4
    }
    
    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
    // some code goes here
    // not necessary for lab1
    
    Page p = pages.get(pid);
    if (p == null)
        return; //not in buffer pool -- doesn't need to be flushed
    
    DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
    file.writePage(p);
    p.markDirty(false, null);
    }
    
    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
    // some code goes here
    // not necessary for labs 1--4
    }
    
    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
    // some code goes here
    // not necessary for lab1
    
    // try to evict a random page, focusing first on finding one that is not dirty
    // currently does not check for pages with uncommitted xacts, which could impact future labs
    Object pids[] = pages.keySet().toArray();
    PageId pid = (PageId) pids[random.nextInt(pids.length)];
    
    try {
        Page p = pages.get(pid);
        if (p.isDirty() != null) { // this one is dirty, try to find first non-dirty
        for (PageId pg : pages.keySet()) {
            if (pages.get(pg).isDirty() == null) {
            pid = pg;
            break;
            }
        }
        }
        flushPage(pid);
    } catch (IOException e) {
        throw new DbException("could not evict page");
    }
    pages.remove(pid);
    }
    
    private class TransactionPermission{

        private TransactionId tid_;
        private Permissions perm_;

        private TransactionPermission(TransactionId tid, Permissions perm){
            tid_ = tid;
            perm_ = perm;

        }

        public TransactionId getTid(){
            return tid_;
        }

        public Permissions getPerm(){
            return perm_;
        }

        public boolean equals(TransactionPermission p) {
            if(tid_.equals(p.getTid()) && perm_.equals(p.getPerm()))
            return true;

            return false;
        }

    }
    /**
     * Manages locks on PageIds held by TransactionIds.
     * S-locks and X-locks are represented as Permissions.READ_ONLY and Permisions.READ_WRITE, respectively
     *
     * All the field read/write operations are protected by this
     * @Threadsafe
     */
    private class LockManager {
    
    final int LOCK_WAIT = 10;       // milliseconds
    Map<TransactionId, ArrayList<PageId>> tidLocks;
    Map<PageId, ArrayList<TransactionId>> pidLocks;
    Map<PageId, ArrayList<TransactionPermission>> pidPerms;
    /**
     * Sets up the lock manager to keep track of page-level locks for transactions
     * Should initialize state required for the lock table data structure(s)
     */
    private LockManager() {
        tidLocks = new HashMap<TransactionId, ArrayList<PageId>>();
        pidLocks = new HashMap<PageId, ArrayList<TransactionId>>();
        pidPerms = new HashMap<PageId, ArrayList<TransactionPermission>>();
        
    }

    public Map<TransactionId, ArrayList<PageId>> getTidLocks(){
        return tidLocks;
    }

    public Map<PageId, ArrayList<TransactionId>> getPidLocks(){
        return pidLocks;
    }

    public Map<PageId, ArrayList<TransactionPermission>> getPidPerms(){
        return pidPerms;
    } 
    
    
    /**
     * Tries to acquire a lock on page pid for transaction tid, with permissions perm. 
     * If cannot acquire the lock, waits for a timeout period, then tries again. 
     * This method does not return until the lock is granted, or an exception is thrown
     *
     * In Exercise 5, checking for deadlock will be added in this method
     * Note that a transaction should throw a DeadlockException in this method to 
     * signal that it should be aborted.
     *
     * @throws DeadlockException after on cycle-based deadlock
     */
    @SuppressWarnings("unchecked")
    public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm)
        throws DeadlockException {
        
        while(!lock(tid, pid, perm)) { // keep trying to get the lock
        
        synchronized(this) {
            // you don't have the lock yet
            // possibly some code here for Exercise 5, deadlock detection
        }
        
        try {
            // couldn't get lock, wait for some time, then try again
            Thread.sleep(LOCK_WAIT); 
        } catch (InterruptedException e) { // do nothing
        }
        
        }
        
        
        synchronized(this) {
        // for Exercise 5, might need some cleanup on deadlock detection data structure
        }
        
        return true;
    }
    
    
    /**
     * Release all locks corresponding to TransactionId tid.
     * This method is used by BufferPool.transactionComplete()
     */
    public synchronized void releaseAllLocks(TransactionId tid) {
       PageId[] dat = new PageId[tidLocks.get(tid).size()];
       dat = tidLocks.get(tid).toArray(dat);
        
        for(PageId p: dat){
            releaseLock(tid, p);
        }
           
    }
    
    
    
    /** Return true if the specified transaction has a lock on the specified page */
    public synchronized boolean holdsLock(TransactionId tid, PageId p) {
        return tidLocks.get(tid).contains(p);
    }
    
    /**
     * Answers the question: is this transaction "locked out" of acquiring lock on this page with this perm?
     * Returns false if this tid/pid/perm lock combo can be achieved (i.e., not locked out), true otherwise.
     * 
     * Logic:
     *
     * if perm == READ_ONLY
     *  if tid is holding any sort of lock on pid, then the tid can acquire the lock (return false).
     *
     *  if another tid is holding a READ lock on pid, then the tid can acquire the lock (return false).
     *  if another tid is holding a WRITE lock on pid, then tid can not currently 
     *  acquire the lock (return true).
     *
     * else
     *   if tid is THE ONLY ONE holding a READ lock on pid, then tid can acquire the lock (return false).
     *   if tid is holding a WRITE lock on pid, then the tid already has the lock (return false).
     *
     *   if another tid is holding any sort of lock on pid, then the tid cannot currenty acquire the lock (return true).
     */
    private synchronized boolean locked(TransactionId tid, PageId pid, Permissions perm) {


        
        if(perm == Permissions.READ_ONLY){
            // if(tidLocks.get(tid) == null){ //this prevents us from getting to other case below
            //     //do nothing
            // }
            //else{
            if(tidLocks.get(tid) != null) {
                if(tidLocks.get(tid).contains(pid)){
                    return false;
                }
            }
            if(pidLocks.get(pid) != null) {
                for(TransactionPermission p: pidPerms.get(pid)){
                    if(!p.getTid().equals(tid)){
                        if(p.getPerm() == Permissions.READ_ONLY){
                            return false;
                        }
                        else if(p.getPerm() == Permissions.READ_WRITE){
                            return true;
                        }
                    }
                }
            }
        }
        //}
        else{
            if(pidPerms.get(pid) == null){
                //do nothing
            }
            else{
                for(TransactionPermission p: pidPerms.get(pid)){
                    if(!p.getTid().equals(tid)){
                        if(p.getPerm() == Permissions.READ_ONLY || p.getPerm() == Permissions.READ_WRITE){
                            return true;
                        } 
                }
                if(p.getTid().equals(tid)){
                    if(p.getPerm() == Permissions.READ_WRITE){
                        return false;
                    }
                }
            
            }
            return false; //are these sketch? unclear.
            
        }
        return false;
    }
return false;
    }

    
    /**
     * Releases whatever lock this transaction has on this page
     * Should update lock table data structure(s)
     *
     * Note that you do not need to "wake up" another transaction that is waiting for a lock on this page,
     * since that transaction will be "sleeping" and will wake up and check if the page is available on its own
     * However, if you decide to change the fact that a thread is sleeping in acquireLock(), you would have to wake it up here
     */
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        tidLocks.get(tid).remove(pid);   
        pidLocks.get(pid).remove(tid);

        //create arraylist of items to remove b/c concurent problem
        ArrayList<TransactionPermission> toRemove = new ArrayList<TransactionPermission>();

        for(TransactionPermission p: pidPerms.get(pid)){
            if(tid.equals(p.getTid())){
                toRemove.add(p);
            }
        }
        pidPerms.get(pid).removeAll(toRemove);
    }
    
    
    /**
     * Attempt to lock the given PageId with the given Permissions for this TransactionId
     * Should update the lock table data structure(s) if successful
     *
     * Returns true if the lock attempt was successful, false otherwise
     */
    private synchronized boolean lock(TransactionId tid, PageId pid, Permissions perm) {
        
        if(locked(tid, pid, perm)) 
        return false; // this transaction cannot get the lock on this page; it is "locked out"
        
        else {

            if (tidLocks.get(tid) == null) {
                tidLocks.put(tid, new ArrayList<PageId>());
                tidLocks.get(tid).add(pid);
            }
            else{
                tidLocks.get(tid).add(pid);
            }

            if (pidLocks.get(pid) == null) {
                pidLocks.put(pid, new ArrayList<TransactionId>());
                pidLocks.get(pid).add(tid);
            }
            else{
                pidLocks.get(pid).add(tid);
            }

            if (pidPerms.get(pid) == null) {
                pidPerms.put(pid, new ArrayList<TransactionPermission>());
                TransactionPermission newTP = new TransactionPermission(tid, perm);
                pidPerms.get(pid).add(newTP);
            }
            else{
               TransactionPermission newTP = new TransactionPermission(tid, perm);
                pidPerms.get(pid).add(newTP);
            }            

            return true;
        }    
        
    }
    }   
    
}