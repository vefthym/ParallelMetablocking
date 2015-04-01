/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package hadoopUtils;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Sorts VIntWritables in descending order
 * @author hduser
 *
 */
public class DescendingVIntComparator extends WritableComparator {
	
	protected DescendingVIntComparator() {
		super(VIntWritable.class, true);
	}
		 
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		VIntWritable k1 = (VIntWritable)w1;
		VIntWritable k2 = (VIntWritable)w2;
		return -1 * k1.compareTo(k2);
	}
	    

}
