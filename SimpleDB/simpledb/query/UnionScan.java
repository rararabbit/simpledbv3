package simpledb.query;

import java.util.HashMap;

//TODO remove duplicates
/**
 * The scan class corresponding to the <i>union</i> relational
 * algebra operator.
 */
public class UnionScan implements Scan {
   private Scan s1, s2;
   private boolean onS1 = true;
   private HashMap<String,String> fieldMatch;
   /**
    * Creates a union scan having the two underlying scans.
    * @param s1 the LHS scan
    * @param s2 the RHS scan
    */
   public UnionScan(Scan s1, Scan s2, HashMap<String,String> fieldMatch) {
      this.s1 = s1;
      this.s2 = s2;
      this.fieldMatch = fieldMatch;
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
    * Moves the scan to the next record.
    * The method moves to the next LHS record until the end
    * and then it moves through the RHS until the end.
    * If there are no more LHS records, the method returns false.
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
      if (onS1 && s1.next())
          return true;
       else {
          onS1 = false;
          return s2.next();
       }
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
      if (onS1)
         return s1.getVal(fldname);
      else
         return s2.getVal(fieldMatch.get(fldname));
   }
   
   /** 
    * Returns the integer value of the specified field.
    * The value is obtained from whichever scan
    * is currently active.
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
   public int getInt(String fldname) {
           if (onS1)
                   return s1.getInt(fldname);
           else
                   return s2.getInt(fieldMatch.get(fldname));
   }
   
   /** 
    * Returns the string value of the specified field.
    * The value is obtained from whichever scan
    * is currently active.
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
   public String getString(String fldname) {
              if (onS1)
                  return s1.getString(fldname);
               else
                  return s2.getString(fieldMatch.get(fldname));
   }
   
   /**
    * Returns true if the specified field is in
    * both of the underlying scans.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return s1.hasField(fldname) && s2.hasField(fieldMatch.get(fldname));
   }
}
