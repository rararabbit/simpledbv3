package simpledb.buffer;

import simpledb.file.*;
import java.util.*;
/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   private int clockPosition;
   private Map<Block,Buffer> bufferPoolMap=new HashMap<Block,Buffer>(); 
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      clockPosition = 0;
      for (int i=0; i<numbuffs; i++)
         bufferpool[i] = new Buffer(i);
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = getMapping(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;     
         buff.assignToBlock(blk);    
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      bufferPoolMap.remove(blk);
      bufferPoolMap.put(blk, buff);
      //System.err.println(toString());
      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      Block blk=buff.assignToNew(filename, fmtr);
      numAvailable--;
      buff.pin();
      bufferPoolMap.put(blk, buff);
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
 
   
   /**
    * Chooses an unpinned buffer using the clock algorithm
    * @return An unpinned buffer
    */
   private Buffer chooseUnpinnedBuffer() {
	   //Are no unpinned buffers to return
	   if (available() == 0)
		   return null;
	   Buffer out = null;
	   Buffer buff = null;
	   while (out == null) {
		   buff = bufferpool[clockPosition];
		   //Find an unpinned buffer
		   if (!buff.isPinned()) {
			   //Check reference flag, if it's clear then return the buffer
			   //otherwise clear the flag
			   if(buff.referenced())
				   buff.clearReference();
			   else {
				   out = buff;
			   }
		   }
		   clockPosition=(clockPosition+1) % bufferpool.length;
	   }
	   //System.err.println(clockPosition);
	   return out;
//      for (Buffer buff : bufferpool)
//         if (!buff.isPinned())
//         return buff;
//      return null;
   }
   
   /**
    * Outputs info for each buffer separated by blank lines
    * @output the string with buffer info
    */
   public String toString(){
	   String out = "";
	   for (Buffer buff : bufferpool) {
		   out = out + buff.toString() + "\n";
	   }
	   return out;
   }
   /**
   * Determines whether the map has a mapping from
   * the block to some buffer.
   * @param blk the block to use as a key
   * @return true if there is a mapping; false otherwise
   */
   boolean containsMapping(Block blk) {
   return bufferPoolMap.containsKey(blk);
   }
   Buffer getMapping(Block blk) {
	   return bufferPoolMap.get(blk);
	   }
   
}
