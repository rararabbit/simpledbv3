package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;

import java.util.*;

/**
 * The Plan class to remove duplicates.
 */
public class RemoveDuplicatesPlan implements Plan {
   private Plan p;
   private Schema sch;
   private Collection<String> sortfields;
   
   /**
    * Creates a plan for the specified query.
    * @param p the plan for the underlying query
    * @param sortfields the fields to determine duplicates by
    * @param tx the calling transaction
    */
   public RemoveDuplicatesPlan(Plan p, Collection<String> sortfields, Transaction tx) {
      this.p = new SortPlan(p, sortfields, tx);
      this.sortfields = sortfields;
      sch = p.schema();
   }
   
   /**
    * Returns only unique values from a sorted scan.
    * @see simpledb.query.Plan#open()
    */
   public Scan open() {
      return new RemoveDuplicatesScan(p.open(), sortfields);
   }
   
   /**
    * Returns the number of blocks in the sorted table,
    * which is the same as the underlying SortPlan
    */
   public int blocksAccessed() {
      return p.blocksAccessed();
   }
   
   /**
    * Returns the number of records in the table,
    * which is the same as in the underlying query (worst case).
    * @see simpledb.query.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput();
   }
   
   /**
    * Returns the number of distinct field values in
    * the table, which is the same as in
    * the underlying query (worst case).
    * @see simpledb.query.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }
   
   /**
    * Returns the schema of the sorted table, which
    * is the same as in the underlying query.
    * @see simpledb.query.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
   
}
