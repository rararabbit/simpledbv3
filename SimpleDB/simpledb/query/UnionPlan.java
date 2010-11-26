package simpledb.query;

import java.util.HashMap;

import simpledb.record.Schema;


/** The Plan class corresponding to the <i>union</i>
 * relational algebra operator.
 * @author Edward Sciore
 */
public class UnionPlan implements Plan {
	private Plan p1, p2;
	private Schema schema = new Schema();
	private HashMap<String,String> fieldMatch = new HashMap<String,String>();

	/**
	 * Creates a new union node in the query tree,
	 * having the two specified subqueries.
	 * @param p1 the left-hand subquery
	 * @param p2 the right-hand subquery
	 */
	public UnionPlan(Plan p1, Plan p2) {
		this.p1 = p1;
		this.p2 = p2;

		//Get fields that are present in both
		for (String field : p1.schema().fields()) {
			if (p2.schema().fields().contains(field) && p1.schema().type(field) == p2.schema().type(field)){
				schema.addField(field, p1.schema().type(field), p1.schema().length(field));
				fieldMatch.put(field, field);
			}
		}
		//Get remaining fields of same type
		for (String field : p1.schema().fields()) {
			//Already in schema, skip
			if (!schema.hasField(field)) {
				int type = p1.schema().type(field);
				for (String field2 : p2.schema().fields()) {
					//Already in schema or of a different type, skip	
					if (!schema.hasField(field2) && p2.schema().type(field2) == type) {
						schema.addField(field, p1.schema().type(field), p1.schema().length(field));
						//Add relation from schema name to actual name
						fieldMatch.put(field, field2);
					}	  
				}
			}
		}

	}

	/**
	 * Creates a union scan for this query.
	 * @see simpledb.query.Plan#open()
	 */
	public Scan open() {
		Scan s1 = p1.open();
		Scan s2 = p2.open();
		return new UnionScan(s1, s2, fieldMatch);
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

	/**
	 * Estimates the number of output records in the union.
	 * The formula is:
	 * <pre> R(union(p1,p2)) = R(p1) + R(p2) </pre>
	 * @see simpledb.query.Plan#recordsOutput()
	 */
	public int recordsOutput() {
		return p1.recordsOutput() + p2.recordsOutput();
	}

	/**
	 * Estimates the distinct number of field values in the union.
	 * Since the union does not increase or decrease field values,
	 * the estimate is the same as in the appropriate underlying queries.
	 * @see simpledb.query.Plan#distinctValues(java.lang.String)
	 */
	public int distinctValues(String fldname) {
		return p1.distinctValues(fldname) + p2.distinctValues(fieldMatch.get(fldname));
	}

	/**
	 * Returns the schema of the union,
	 * @see simpledb.query.Plan#schema()
	 */
	public Schema schema() {
		return schema;
	}
}
