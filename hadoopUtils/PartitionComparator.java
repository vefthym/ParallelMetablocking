package hadoopUtils;

import java.util.Comparator;

public class PartitionComparator implements Comparator<Partition> {

	@Override
	public int compare(Partition partition1, Partition partition2) {
		return new Long(partition1.getTotalComparisons()).compareTo(partition2.getTotalComparisons());
	}
	
	

}
