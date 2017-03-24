package simpledb;
import java.util.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    public int buckets_;
    public int min_;
    public int max_;
    public int numInBucket; //represents the number of values in the range of each bucket
    public Integer[] bucketList;
    public int ntups;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * Note: if the number of buckets exceeds the number of distinct integers between min and max, 
     * some buckets may remain empty (don't create buckets with non-integer widths).
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	buckets_ = buckets;
        min_ = min;
        max_ = max;
        bucketList = new Integer[buckets_];
        for (int i=0; i < bucketList.length; i++)
            bucketList[i] = 0;
        ntups=0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) { 

     if(max_<min_) {

     }
     if(v>=min_ && v<=max_){
         if (buckets_ == 0){
            //do nothing
        }
        else{
        ntups++;
        int difference = max_-min_;

        //this computes the number of values in the range that should go in the bucket.
        //it's honestly just a sketchy way of computing ceiling(difference/buckets_) and then dividing that by buckets_
        numInBucket = ((difference + buckets_ - 1)/buckets_);
        
            if (numInBucket == 0){
                //do nothing
            }
            else{
                //the index where we will add is the ceiling of v/buckets
                //int ourIndex = (((v+numInBucket-1)/numInBucket)-1);

                int ourIndex;
                if (v == max_){
                    ourIndex = buckets_ -1;
                }
                else{
                    ourIndex = (v-min_)/numInBucket;
                }
                
                

                //int ourIndex = 5;
                //add our value to the spot in the array corresponding to that index
                bucketList[ourIndex]++;
                
            }
       
        }

    
}
}
 
    	

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    int difference = max_-min_;
    
    if (buckets_!=0){
        int numInThisBucket = ((difference + buckets_ - 1)/buckets_);
        // System.out.println("numinBucket");
        // System.out.println(numInThisBucket);
        int thisIndex;
                if (v == max_){
                    thisIndex = buckets_ -1;
                }
                else{
                    thisIndex = (v-min_)/numInBucket;
                }

        double b_right;
        double b_part;
        double heightsOfRest;
        double b_left;

    
 
        // System.out.println("index before switch");
        // System.out.println(thisIndex);
            if(op.equals(Predicate.Op.EQUALS)) {
               return ((double)bucketList[thisIndex] / numInThisBucket) / ntups;
               
           }

           else if(op.equals(Predicate.Op.LIKE)){
               return ((double)bucketList[thisIndex] / numInThisBucket) / ntups;
               
           }
            
            else if(op.equals(Predicate.Op.GREATER_THAN)){
                System.out.println("greater_than case");
                    // System.out.println(v);
                    // System.out.println(min_);
                if(v < min_){
                    
                    return 1.00;
                    

                }
                else if (v > max_){

                    return 0.00;
                    
                }
                else{
                    //difference betw v and min 
                    //b_right = numInThisBucket * (thisIndex+1);
                    b_right = min_ + numInThisBucket * thisIndex + (numInThisBucket - 1);
                    b_part = (b_right - v)/numInThisBucket;
                    heightsOfRest = 0.0;
                    //System.out.println("before for loop");
                    for(int i = thisIndex+1; i<bucketList.length; i++) {
                        //System.out.println(i);
                        heightsOfRest+=bucketList[i];
                    }
                    return (b_part + heightsOfRest)/ntups;
                   
                } 
            }
                
           else if(op.equals(Predicate.Op.LESS_THAN)) {
               System.out.println("less_than case");

                if(v < min_){
                    
                    return 0.00;
                    

                }
                else if (v > max_){

                    return 1.00;
                    
                }
                
                b_left = min_ + numInThisBucket * thisIndex;
                b_part = (v-b_left)/numInThisBucket;
                heightsOfRest = 0.0;
                for(int i = thisIndex-1; i >=0; i--) {
                    heightsOfRest+=bucketList[i];
                }
                return (b_part + heightsOfRest)/ntups;
                
            }
            else if(op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
                System.out.println("less_than or eq case");
                 if(v < min_){
                    
                    return 0.00;
                    

                }
                else if (v > max_){

                    return 1.00;
                    
                }
                b_left = numInThisBucket * thisIndex + 1;
                
                b_part = (v-b_left)/numInThisBucket;
                heightsOfRest = 0.0;
                for(int i = thisIndex-1; i >=0; i--) {
                    heightsOfRest+=bucketList[i];
                }
                //return (b_part + heightsOfRest)/ntups;
                return (b_part + heightsOfRest + ((double)bucketList[thisIndex] / numInThisBucket))/ntups;
                
            }
            
            if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
                System.out.println("greater_than or eq case");

                 if(v < min_){
                    
                    return 1.00;
                    

                }
                else if (v > max_){

                    return 0.00;
                    
                }
                else{
                b_right = min_ + numInThisBucket * thisIndex + (numInThisBucket - 1);
                    b_part = (b_right - v)/numInThisBucket;
                    heightsOfRest = 0.0;
                    //System.out.println("before for loop");
                    for(int i = thisIndex+1; i<bucketList.length; i++) {
                        //System.out.println(i);
                        heightsOfRest+=bucketList[i];
                    }
                    return (b_part + heightsOfRest + ((double)bucketList[thisIndex] / numInThisBucket))/ntups;
                   
            }
                
            }
                
            else if(op.equals(Predicate.Op.NOT_EQUALS)){
                return 1-((double)bucketList[thisIndex] / numInThisBucket) / ntups;
                
            }

        
            
    }
    
    //System.out.println("about to return 0");
    return 0.0;
}
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It could be used to
     *     implement a more efficient optimization
     *
     * Not necessary for lab 3
     * */
    public double avgSelectivity()
    {
        return 0.5;
    }
    
    /**
     * (Optional) A String representation of the contents of this histogram
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
