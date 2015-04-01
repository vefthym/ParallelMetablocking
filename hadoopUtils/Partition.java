/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package hadoopUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Partition {
	
	private List<Integer> blockIds;
	private long totalComparisons;
	
	public Partition() {
		blockIds = new ArrayList<>();
		totalComparisons = 0;
	}
	
	/**
	 * 
	 * @param block a Map entry, whose key is a block Id and value is the num of comparisons in this block
	 */
	public void addBlock(Map.Entry<Integer, Long> block) {
		blockIds.add(block.getKey());
		totalComparisons += block.getValue();
	}
	
	public long getTotalComparisons() {
		return totalComparisons;
	}
	
	public List<Integer> getBlockIds() {
		return blockIds;
	}
	
	

}
