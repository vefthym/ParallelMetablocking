/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class EntityIndexReducerARCS extends MapReduceBase implements Reducer<VIntWritable, VIntArrayWritable, VIntWritable, VIntArrayWritable> {
	
	
	static enum OutputData {D1Entities, D2Entities, BLOCK_ASSIGNMENTS, TEST}; 
	
	Map<Integer,Integer> blockUtils; //(blockId, rank) - rank is based on utility
	private Path[] localFiles;


	public void configure(JobConf job){	
		
		blockUtils = new HashMap<>();		
		int blockRank = 0; //based on its relative position in the sorted list by utility 
		
		BufferedReader SW;
		try {
			localFiles = DistributedCache.getLocalCacheFiles(job); 	//blocks sorted by utility (descending)		
			SW = new BufferedReader(new FileReader(localFiles[0].toString()));
			String line;
			while ((line = SW.readLine()) != null) {
				Integer block = Integer.parseInt(line.substring(line.indexOf("\t")+1)); //line has the form: utility+"\t"+blockId
				blockUtils.put(block, blockRank++);		//blocks are already sorted		
			}
		    SW.close();
		} catch (FileNotFoundException e) {
			System.err.println(e.toString());
		} catch (IOException e) {
			System.err.println(e.toString());
		}		
	}
	
	
	/**
	 * Builds the Entity Index, after performing Block Filtering
	 * To skip the block filtering part, just output all the blocks and not the top MAX_BLOCKS
	 * by commenting out the specified line
	 * @param _key entity id
	 * @param values list of [blockId,blockSize] of the current entity 
	 * @param output key: same as input key. value: [blockId,blockSize,blockId,blockSize,...] for retained blocks
	 */
	public void reduce(VIntWritable _key, Iterator<VIntArrayWritable> values,
			OutputCollector<VIntWritable, VIntArrayWritable> output, Reporter reporter) throws IOException {
				
		//store the blocks of this entity in ascending order of utility rank
		//since each block has a unique rank, this rank can be used as a new block id
		Map<Integer,Integer> blocks = new TreeMap<>(); //TreeSet keeps the Set sorted (ascending)
		
		while (values.hasNext()) {
			VIntArrayWritable value = values.next();
			Integer block = value.get()[0].get(); //the block id 
			Integer rank = blockUtils.get(block); //the global rank of this block, based on its utility
			if (rank != null) {
				blocks.put(rank,value.get()[1].get()); //store the block using its rank as an id (key of map) and its size as the value of map 				
			}
		}	
		
		
		//local threshold for block filtering
		//final int MAX_BLOCKS = ((Double)Math.floor(blocks.size()/3+1)).intValue(); //|_ |Bi|/3+1 _| //graph-free
		final int MAX_BLOCKS = ((Double)Math.floor(3*blocks.size()/4+1)).intValue(); //|_ 3|Bi|/4+1 _| //preprocessing
//		final int MAX_BLOCKS = ((Double)Math.floor(3*blocks.size()/4)).intValue(); //|_ 3|Bi|/4+1 _| //preprocessing
		
		Map<VIntWritable, VIntWritable> toEmit = new TreeMap<>();		
				
		int indexedBlocks = 0;
		for (Map.Entry<Integer, Integer> block : blocks.entrySet()) { //returned in ascending order of rank (highest utility->rank 0)
			toEmit.put(new VIntWritable(block.getKey()), new VIntWritable(block.getValue()));
			if (++indexedBlocks == MAX_BLOCKS) { break;} //comment-out this line to skip block filtering
		} 
		
		//transform the set to an array, which will be the final output (toEmitFinal)
		VIntWritable[] toEmitArray = new VIntWritable[toEmit.size()*2];
		int index = 0;
		for (Map.Entry<VIntWritable, VIntWritable> block : toEmit.entrySet()) { //returned in ascending order of rank (highest utility->rank 0)
			toEmitArray[index++] = block.getKey();
			toEmitArray[index++] = block.getValue();
		}				
		VIntArrayWritable toEmitFinal = new VIntArrayWritable(toEmitArray);
		
		//VIntArrayWritable toEmitFinal = hadoopUtils.RelativePositionCompression.compress(toEmit);
		
		if (indexedBlocks > 0) {
			if (_key.get() >= 0) {
				reporter.incrCounter(OutputData.D1Entities, 1);
			} else {
				reporter.incrCounter(OutputData.D2Entities, 1);
			}
			output.collect(_key, toEmitFinal);
			reporter.incrCounter(OutputData.BLOCK_ASSIGNMENTS, toEmit.size());
			//BC = BLOCK_ASSIGNMENTS / REDUCE_OUTPUT_RECORDS;
		} 	//else skip this entity (it is not placed in any block)
	}
	

}
