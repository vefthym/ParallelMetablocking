package Temp.BigData.LoadBalancing;

import DataStructures.AbstractBlock;
import EfficiencyLayer.AbstractEfficiencyMethod;
import Utilities.StatisticsUtilities;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 *
 * @author G.A.P. II
 */
public class LoadBalancing extends AbstractEfficiencyMethod {

    private final int noOfBlocks;
    private double partitionComparisons;//maximum comparisons per partition
    private final List<AbstractBlock> blocks;
    private Partition seedPartition;
    private final Queue<Partition> priorityQueue;

    public LoadBalancing(List<AbstractBlock> bls) {
        super("Hadoop Load Balancing");
        blocks = bls;
        noOfBlocks = blocks.size();
        priorityQueue = new PriorityQueue<Partition>(blocks.size(), new PartitionComparator());
    }

    protected void addSeedPartition() {
        //one partition for the largest block
        AbstractBlock largestBlock = blocks.remove(0);
        seedPartition = new Partition();
        seedPartition.addBlock(largestBlock);
        priorityQueue.add(seedPartition);
    }

    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
        sortBlocks();
        addSeedPartition();
        setPartitionComparisons();
        partitionBlocks();
    }

    public List<Partition> getPartitions() {
        return new ArrayList<Partition>(priorityQueue);
    }

    protected void partitionBlocks() {
        while (!blocks.isEmpty()) {
            AbstractBlock currentBlock = blocks.remove(0);
            Partition smallestPartition = priorityQueue.poll();
            double totalComparisons = smallestPartition.getTotalComparisons() + currentBlock.getNoOfComparisons();
            if (totalComparisons <= partitionComparisons) { // in the new block fits into the smallest partition
                smallestPartition.addBlock(currentBlock); //add it to the partition
            } else { //otherwise create a new partition for the current block
                Partition newPartition = new Partition();
                newPartition.addBlock(currentBlock);
                priorityQueue.add(newPartition);
            }
            priorityQueue.add(smallestPartition);
            
            if (blocks.isEmpty()) {
                smallestPartition = priorityQueue.poll();
                if (smallestPartition.getTotalComparisons() < 0.9*partitionComparisons && 
                        (int)(0.5*noOfBlocks) < smallestPartition.getSize()) {
                    blocks.addAll(smallestPartition.getBlocks());
                    partitionComparisons += smallestPartition.getTotalComparisons()/priorityQueue.size()+1;
                } else {
                    priorityQueue.add(smallestPartition);
                }
            }
        }
    }

    public void printPartitionStats() {
        double totalComparisons = 0;
        final ArrayList<Double> partitionBlocks = new ArrayList<>();
        final ArrayList<Double> partitionComparisons = new ArrayList<>();
        int noOfPartitions = priorityQueue.size();
        for (int i = 0; i < noOfPartitions; i++) {
            Partition partition = priorityQueue.poll();
            partitionBlocks.add(new Double(partition.getSize()));
            partitionComparisons.add(partition.getTotalComparisons());
            totalComparisons += partition.getTotalComparisons();
        }
        
        System.out.println("\n\nTotal comparisons\t:\t" + totalComparisons);
        System.out.println("Total partitions\t:\t" + noOfPartitions);
        System.out.println("\n\n");
        double avComparisons = StatisticsUtilities.getMeanValue(partitionComparisons);
        System.out.println("Maximum partition comparisons\t:\t" + StatisticsUtilities.getMaxValue(partitionComparisons));
        System.out.println("Median partition comparisons\t:\t" + StatisticsUtilities.getMedianValue(partitionComparisons));
        System.out.println("Mean partition comparisons\t:\t" + avComparisons + "+-" + StatisticsUtilities.getStandardDeviation(avComparisons, partitionComparisons));
        System.out.println("Mininimum partition comparisons\t:\t" + StatisticsUtilities.getMinValue(partitionComparisons));
        System.out.println("\n\n");
        double avBlocks = StatisticsUtilities.getMeanValue(partitionBlocks);
        System.out.println("Maximum partition blocks\t:\t" + StatisticsUtilities.getMaxValue(partitionBlocks));
        System.out.println("Median partition blocks\t:\t" + StatisticsUtilities.getMedianValue(partitionBlocks));
        System.out.println("Mean partition blocks\t:\t" + avBlocks + "+-" + StatisticsUtilities.getStandardDeviation(avBlocks, partitionBlocks));
        System.out.println("Mininimum partition blocks\t:\t" + StatisticsUtilities.getMinValue(partitionBlocks));
    }

    protected void setPartitionComparisons() {
        partitionComparisons = seedPartition.getTotalComparisons();
    }

    protected void sortBlocks() {
        //Sort from the largest to the smallest
        Collections.sort(blocks, new InverseBlockCardinalityComparator());
    }
}
