package simpledb.materialize;

import simpledb.query.*;
import java.util.*;

/**
 * The Scan class for the <i>sort</i> operator.
 * @author Edward Sciore
 */
/**
 * @author sciore
 *
 */
public class RemoveDuplicatesScan implements Scan {
   private Scan s1;
   private HashMap<String,Constant> previous = new HashMap<String,Constant>();
   private Collection<String> sortfields;
   
   /**
    * Creates a scan
    * @param s the underlying scan
    * @param sortfields The fields to compare for duplicates
    */
   public RemoveDuplicatesScan(Scan s, Collection<String> sortfields) {
      s1 = s;
      this.sortfields = sortfields;
    }
   
   /**
    * Positions the scan before the first record
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      s1.beforeFirst();
   }
   
   /**
    * Moves to the next record, skipping records that are the
    * same as the previous one.  These are duplicate records since
    * the list is sorted.
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
	   //If there are no more records, leave
	   if (!s1.next())
		   return false;
	   //There are more records
	   else {
		   //See if record is the same as the one before it
		   //This removes duplicates since the list is sorted
		   do {
			   int equalFields=0,totalFields=0;
			   for(String field: sortfields){
				   Constant temp = s1.getVal(field);
				   if (previous.get(field) != null && previous.get(field).compareTo(temp) == 0){
					   equalFields++;	
				   }
				   totalFields++;
				   previous.remove(field);
				   previous.put(field, s1.getVal(field));
			   }
			   //At least one field was different
			   if (equalFields != totalFields)
				   return true;
			   //All fields were the same, goto next record
		   } while (s1.next());
	   }
	   //Left loop without findng a unique record
	   return false;
   }
   
   /**
    * Closes the underlying scan.
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      s1.close();
   }
   
   /**
    * Gets the Constant value of the specified field
    * of the scan.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      return s1.getVal(fldname);
   }
   
   /**
    * Gets the integer value of the specified field
    * of the scan.
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
   public int getInt(String fldname) {
      return s1.getInt(fldname);
   }
   
   /**
    * Gets the string value of the specified field
    * of the scan.
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
   public String getString(String fldname) {
      return s1.getString(fldname);
   }
   
   /**
    * Returns true if the specified field is in the current scan.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return s1.hasField(fldname);
   }
 
}
