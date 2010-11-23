package simpledb.query;

import java.util.Collection;

import simpledb.materialize.RecordComparator;

//TODO remove duplicates
/**
 * The scan class corresponding to the <i>union</i> relational
 * algebra operator.
 */
public class UnionScan implements Scan {
	private Scan s1;
	private Scan s2,currentscan;
    Collection<String> fldname1, fldname2;
    private boolean hasmore1, hasmore2=false;
    private RecordComparator comp;
   /**
    * Creates a union scan having the two underlying scans.
    * @param s1 the LHS scan
    * @param s2 the RHS scan
    */
   public UnionScan(Scan s1, Scan s2, Collection<String> fldname1, Collection<String> fldname2) {
	      this.s1 = s1;
	      this.s2 = s2;
	      this.fldname1 = fldname1;
	      this.fldname2 = fldname2;
	      beforeFirst();
	   }
   
   
   /**
    * Positions the scan before its first record.
    * Both the LHS and RHS are positioned before their
    * first records.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      s1.beforeFirst();
      s2.beforeFirst();
   }
   
   /**
    * Moves to the next record in sorted order.
    * First, the current scan is moved to the next record.
    * Then the lowest record of the two scans is found, and that
    * scan is chosen to be the new current scan.
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
	   if (currentscan != null) {
	         if (currentscan == s1)
	            hasmore1 = s1.next();
	         else if (currentscan == s2)
	            hasmore2 = s2.next();
	      }
	      
	      if (!hasmore1 && !hasmore2)
	         return false;
	      else if (hasmore1 && hasmore2) {
	         if (comp.compare(s1, s2) < 0)
	            currentscan = s1;
	         else if(comp.compare(s1, s2) == 0){
	        	 currentscan=s1;
	        	 s2.next();
	         }
	         else
	            currentscan = s2;
	      }
	      else if (hasmore1)
	         currentscan = s1;
	      else if (hasmore2)
	         currentscan = s2;
	      return true;
		 
	   }
   
   /**
    * Closes both underlying scans.
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      s1.close();
      s2.close();
   }
   
   /** 
    * Returns the value of the specified field.
    * The value is obtained from whichever scan
    * is currently active.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
	      return currentscan.getVal(fldname);
	   }
	   
	   /**
	    * Gets the integer value of the specified field
	    * of the current scan.
	    * @see simpledb.query.Scan#getInt(java.lang.String)
	    */
	   public int getInt(String fldname) {
	      return currentscan.getInt(fldname);
	   }
	   
	   /**
	    * Gets the string value of the specified field
	    * of the current scan.
	    * @see simpledb.query.Scan#getString(java.lang.String)
	    */
	   public String getString(String fldname) {
	      return currentscan.getString(fldname);
	   }
   
   /**
    * Returns true if the specified field is in
    * both of the underlying scans.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
	   public boolean hasField(String fldname) {
		      return currentscan.hasField(fldname);
		   }
}
