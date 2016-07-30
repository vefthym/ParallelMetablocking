/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package hadoopUtils;

import java.util.HashMap;
import java.util.Map;

public class Partition {
	
	private Map<Integer,Long> blocks;
	private long totalComparisons;
	
	public Partition() {
		blocks = new HashMap<>();
		totalComparisons = 0;
	}
	
	/**
	 * 
	 * @param block a Map entry, whose key is a block Id and value is the num of comparisons in this block
	 */
	public void addBlock(Map.Entry<Integer, Long> block) {
		blocks.put(block.getKey(),block.getValue());
		totalComparisons += block.getValue();
	}
	
	public long getTotalComparisons() {
		return totalComparisons;
	}
	
	public Map<Integer,Long> getBlocks() {
		return blocks;
	}
	
	

}
