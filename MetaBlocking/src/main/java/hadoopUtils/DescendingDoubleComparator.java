/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package hadoopUtils;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Sorts DoubleWritables in descending order
 * @author hduser
 *
 */
public class DescendingDoubleComparator extends WritableComparator {
	
	protected DescendingDoubleComparator() {
		super(DoubleWritable.class, true);
	}
		 
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		DoubleWritable k1 = (DoubleWritable)w1;
		DoubleWritable k2 = (DoubleWritable)w2;
		return -1 * k1.compareTo(k2);
	}
	    

}
