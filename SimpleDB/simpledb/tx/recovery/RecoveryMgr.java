package simpledb.tx.recovery;

import static simpledb.tx.recovery.LogRecord.*;
import simpledb.file.Block;
import simpledb.buffer.Buffer;
import simpledb.server.SimpleDB;
import java.util.*;
import simpledb.tx.Transaction;

/**
 * The recovery manager.  Each transaction has its own recovery manager.
 * @author Edward Sciore
 */
public class RecoveryMgr {
   private int txnum;

   /**
    * Creates a recovery manager for the specified transaction.
    * @param txnum the ID of the specified transaction
    */
   public RecoveryMgr(int txnum) {
      this.txnum = txnum;
      new StartRecord(txnum).writeToLog();
   }

   /**
    * Writes a commit record to the log, and flushes it to disk.
    */
   public void commit() {
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new CommitRecord(txnum).writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }

   /**
    * Writes a rollback record to the log, and flushes it to disk.
    */
   public void rollback() {
      doRollback();
      SimpleDB.bufferMgr().flushAll(txnum);
      int lsn = new RollbackRecord(txnum).writeToLog();
      SimpleDB.logMgr().flush(lsn);
   }

   /**
    * Recovers uncompleted transactions from the log,
    * then creates a nonquiescent checkpoint record 
    * by passing the list of active transactions
    * to the constructor NQCheckpoint 
    * and writes the record to the log and flushes it.
    */
   public void recover() {
      doRecover();
      SimpleDB.bufferMgr().flushAll(txnum);
     // int lsn = new CheckpointRecord().writeToLog();
      int lsn=new NQCheckpoint(Transaction.getActive()).writeToLog();
      SimpleDB.logMgr().flush(lsn);

   }

   /**
    * Writes a setint record to the log, and returns its lsn.
    * Updates to temporary files are not logged; instead, a
    * "dummy" negative lsn is returned.
    * @param buff the buffer containing the page
    * @param offset the offset of the value in the page
    * @param newval the value to be written
    */
   public int setInt(Buffer buff, int offset, int newval) {
      int oldval = buff.getInt(offset);
      Block blk = buff.block();
      if (isTempBlock(blk))
         return -1;
      else
         return new SetIntRecord(txnum, blk, offset, oldval).writeToLog();
   }

   /**
    * Writes a setstring record to the log, and returns its lsn.
    * Updates to temporary files are not logged; instead, a
    * "dummy" negative lsn is returned.
    * @param buff the buffer containing the page
    * @param offset the offset of the value in the page
    * @param newval the value to be written
    */
   public int setString(Buffer buff, int offset, String newval) {
      String oldval = buff.getString(offset);
      Block blk = buff.block();
      if (isTempBlock(blk))
         return -1;
      else
         return new SetStringRecord(txnum, blk, offset, oldval).writeToLog();
   }

   /**
    * Rolls back the transaction.
    * The method iterates through the log records,
    * calling undo() for each log record it finds
    * for the transaction,
    * until it finds the transaction's START record.
    */
   private void doRollback() {
      Iterator<LogRecord> iter = new LogRecordIterator();
      while (iter.hasNext()) {
         LogRecord rec = iter.next();
         if (rec.txNumber() == txnum) {
            if (rec.op() == START)
               return;
            rec.undo(txnum);
         }
      }
   }

   /**
    * Does a complete database recovery.
    * The method iterates through the log records.
    * Whenever it finds a log record for an unfinished
    * transaction, it calls undo() on that record.
    * Whenever it finds a CHECKPOINT record it looks
    * for the first uncommitted transaction in the list
    * of active transactions and set it as the checkpoint transaction
    * It does the recovery procedure until it encounters
    * the start record of the checkpoint transaction
    * or the end of the log.
    */
   private void doRecover() {
	   ArrayList<Integer> recActiveTransactions=null;
	   Collection<Integer> finishedTxs = new ArrayList<Integer>();
	   Iterator<LogRecord> iter = new LogRecordIterator();
	   while (iter.hasNext()) {
		   LogRecord rec = iter.next();
		   System.out.println(rec);
		   if(rec.op()==CHECKPOINT) {
			   recActiveTransactions=rec.getactiveTrans();
			   //Were no active transactions at time of checkpoint
			   if(recActiveTransactions==null || recActiveTransactions.size() == 0)
				   return;
		   } else if (rec.op() == COMMIT || rec.op() == ROLLBACK)
			   finishedTxs.add(rec.txNumber());
		   else if(rec.op()==START){
			   //Find the earliest START of an active transaction after checkpoint has been encountered
			   if (!(recActiveTransactions==null)){
				   recActiveTransactions.remove((Integer)rec.txNumber());
				   if (recActiveTransactions.size() == 0)
					   return;
			   }
		   }      	 
		   else if (!finishedTxs.contains(rec.txNumber()))
			   rec.undo(txnum);
	   }
   }

   /**
    * Determines whether a block comes from a temporary file or not.
    */
   private boolean isTempBlock(Block blk) {
      return blk.fileName().startsWith("temp");
   }
   
   
   /**
    * Flushes buffers to disk and writes a nonquiescent checkpoint record
    */
   public void checkpoint(){
	   ArrayList<Integer> active = Transaction.getActive();
	   for(Integer acttr:active) {
		   SimpleDB.bufferMgr().flushAll(acttr);
	   }
	   int lsn=new NQCheckpoint(active).writeToLog();
	   SimpleDB.logMgr().flush(lsn);
   }
}
