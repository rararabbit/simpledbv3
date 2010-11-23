package simpledb.query;

import java.util.Collection;
import simpledb.materialize.SortPlan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;


/** The Plan class corresponding to the <i>union</i>
  * relational algebra operator.
  * @author Edward Sciore
  */
public class UnionPlan implements Plan {
	 private Plan p1, p2;
	   private Collection<String> fldname1, fldname2;
	   private Schema sch = new Schema();
   
   /**
    * Creates a new union node in the query tree,
    * having the two specified subqueries.
    * @param p1 the left-hand subquery
    * @param p2 the right-hand subquery
    */
   public UnionPlan(Plan p1, Plan p2,Transaction tx) {
      this.p1 = p1;
      this.p2 = p2;
      
      //Only want fields that are present in both
      Collection<String> sortlist1 =p1.schema().fields();
      this.p1 = new SortPlan(p1, sortlist1, tx);
      
      Collection<String> sortlist2 =p2.schema().fields();
      this.p2 = new SortPlan(p2, sortlist2, tx);
      
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
      
   }
   
   /**
    * Creates a union scan for this query.
    * @see simpledb.query.Plan#open()
    */
   public Scan open() {
	      Scan s1 = p1.open();
	      Scan s2 =  p2.open();
	      return new UnionScan(s1, s2, fldname1, fldname2);
	   }
   
   /**
    * Estimates the number of block accesses in the union.
    * The formula is:
    * <pre> B(union(p1,p2)) = B(p1) + B(p2) </pre>
    * @see simpledb.query.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return p1.blocksAccessed() + p2.blocksAccessed();
   }
 //TODO
   /**
    * Estimates the number of output records in the union.
    * The formula is:
    * <pre> R(union(p1,p2)) = R(p1) + R(p2) </pre>
    * @see simpledb.query.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p1.recordsOutput() + p2.recordsOutput();
   }
 //TODO
   /**
    * Estimates the distinct number of field values in the product.
    * Since the product does not increase or decrease field values,
    * the estimate is the same as in the appropriate underlying query.
    * @see simpledb.query.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      if (p1.schema().hasField(fldname))
         return p1.distinctValues(fldname);
      else
         return p2.distinctValues(fldname);
   }

   /**
    * Returns the schema of the union,
    * which is the intersection of the schemas of the underlying queries.
    * @see simpledb.query.Plan#schema()
    */
   public Schema schema() {
	  return sch;
   }
}
