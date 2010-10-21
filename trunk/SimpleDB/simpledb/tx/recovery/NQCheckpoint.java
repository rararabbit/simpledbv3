package simpledb.tx.recovery;

import java.util.ArrayList;

import simpledb.log.BasicLogRecord;



public class NQCheckpoint implements LogRecord {
       private ArrayList<Integer> activeTransacts=null;
       private ArrayList<Object> chkPoint=null;
       /**creates a nonquiescent checkpoint record
        * 
        * @param list of activeTransactions
        */
	   public NQCheckpoint(ArrayList<Integer> activeTrans){
		   this.activeTransacts=activeTrans;
	   }
	   /**creates the list of active transactions
	    * associated with the check point record
	    * by reading the values from log record
	    * @param rec
	    */
	   public NQCheckpoint(BasicLogRecord rec) {
		   int size=rec.nextInt();
		   for(int i=1;i<=size;i++)
		   {
		   activeTransacts.add(rec.nextInt());
		   }
	   }
	   /**Writes a nonquiescent check point record 
	    * to the log,This log record contains the 
	    * checkpoint operator and list of active
	    * transactions at that time.
	    */
	   public int writeToLog() {
		   chkPoint.add(CHECKPOINT);
		   chkPoint.add(activeTransacts.size());
		   for(Integer acttr:activeTransacts)
		   {
			   chkPoint.add(acttr);
		   }
	      Object[] rec = chkPoint.toArray();
	      return logMgr.append(rec);
	   }
	   
	   public int op() {
	      return CHECKPOINT;
	   }
	   
	   /**
	    * Checkpoint records have no associated transaction,
	    * and so the method returns a "dummy", negative txid.
	    */
	   public int txNumber() {
	      return -1; // dummy value
	   }
	   
	   /**
	    * Does nothing, because a checkpoint record
	    * contains no undo information.
	    */
	   public void undo(int txnum) {}
	   
	   public String toString() {
	      return "<CHECKPOINT>";
	   }
	   /**Returns list of active transactions
	    * associated with checkpoint record
	    * 
	    */
	   public ArrayList<Integer> getactiveTrans(){
		return activeTransacts;
		}
}
