/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package hadoopUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.VIntWritable;


public class MBTools {
	
	public enum WeightingScheme {CBS, ECBS, ARCS, JS, EJS}; 
	
	/**
	 * the inverse of Arrays.toString(), returning a list
	 * @param stringArray the result of Arrays.toString()
	 * @return the list represenation of the array that was given as parameter in the original Arrays.toString() method
	 */
	public static List<Integer> listFromStringArray(String stringArray) {
		List<Integer> result = new ArrayList<>();		
		String[] blocks = stringArray.substring(1, stringArray.lastIndexOf("]")).split(", ");	    
	    for (String block : blocks) {
	    	result.add(Integer.parseInt(block));	      
	    }
	    return result;
	}

	public static boolean isRepeated(List<Integer> blocks1, List<Integer> blocks2, int blockIndex) {		
		 for (int i = 0; i < blocks1.size(); ++i) {
			 int block1i = blocks1.get(i);
			 if (i == 0 && block1i == blockIndex) { //first block in index => cannot be repeated
				 return false;
			 }
			 for (int j = 0; j < blocks2.size(); ++j) {
				 int block2j = blocks2.get(j);
				 if (j == 0 && block2j == blockIndex) { //first block in index => cannot be repeated
					 return false;
				 }
				 if (block2j < block1i) {
					 continue;
				 }
				 if (block1i < block2j) {
					 break;
				 }
				 if (block1i == block2j) {
					 return block1i != blockIndex;
				 }
			 }
		 }
		 //System.err.println("Error!!!!"); //since this check comes from a common block
		 return false;
	 } 
	
	public static boolean isRepeated(List<Integer> blocks1, List<Integer> blocks2, int blockIndex, String weightingScheme) {
		if (weightingScheme.equals("ARCS")) {
			List<Integer> newBlocks1 = new ArrayList<>(blocks1.size()/2);
			for (int i = 0; i < blocks1.size(); i+=2) {
				newBlocks1.add(blocks1.get(i));
			}
			List<Integer> newBlocks2 = new ArrayList<>(blocks2.size()/2);
			for (int i = 0; i < blocks2.size(); i+=2) {
				newBlocks2.add(blocks2.get(i));
			}
			return isRepeated(newBlocks1, newBlocks2, blockIndex);
		} else if (weightingScheme.equals("EJS")) {
			//TODO: fill this code
			assert(false); //not implemented yet
			return isRepeated(blocks1, blocks2, blockIndex);
		} else {
			return isRepeated(blocks1, blocks2, blockIndex);   
		}
	 } 
	
	 public static boolean isRepeated(Integer[] blocks1, Integer[] blocks2, int blockIndex) {		
		 for (int i = 0; i < blocks1.length; ++i) {
			 Integer block1i = blocks1[i];
			 if (i == 0 && block1i == blockIndex) { //first block in index => cannot be repeated
				 return false;
			 }
			 for (int j = 0; j < blocks2.length; ++j) {
				 Integer block2j = blocks2[j];
				 if (j == 0 && block2j == blockIndex) { //first block in index => cannot be repeated
					 return false;
				 }
				 if (block2j < block1i) {
					 continue;
				 }
				 if (block1i < block2j) {
					 break;
				 }
				 if (block1i == block2j) {
					 return block1i != blockIndex;
				 }
			 }
		 }
		 //System.err.println("Error!!!!"); //since this check comes from a common block
		 assert(false) : "Error!No common blocks for two entities of the same block!";
		 return false;
	 } 
	
	public static double getWeight(List<Integer> blocks1, List<Integer> blocks2, int blockIndex, String weightingScheme) {
		return getWeight(blocks1, blocks2, blockIndex, weightingScheme,	0, 0);
	}

	public static double getWeight(List<Integer> blocks1, List<Integer> blocks2, int blockIndex, String weightingScheme, int totalBlocks) {		
		return getWeight(blocks1, blocks2, blockIndex, weightingScheme,	totalBlocks, 0);
	}

	public static double getWeight(List<Integer> blocks1, List<Integer> blocks2, int blockIndex, String weightingScheme, int totalBlocks, long validComparisons) {
		 switch (weightingScheme) {
		 case "ARCS":						 
			 final Map<Integer,Integer> commonIndices = getCommonBlockIndicesARCS(blocks1, blocks2);
			 if (commonIndices == null) {
				 return -1;
			 }
			 double totalWeight = 0;
			 for (Map.Entry<Integer, Integer> commonBlock : commonIndices.entrySet()) {
				 totalWeight += 1.0 / commonBlock.getValue();
			 }
			 return totalWeight;
		 case "CBS":
			 return getNoOfCommonBlocks(blocks1, blocks2);
		 case "ECBS":
			 double commonBlocks = getNoOfCommonBlocks(blocks1, blocks2);
			 if (commonBlocks < 0) {
				 return commonBlocks;
			 }
		 	 return commonBlocks * Math.log10((double)totalBlocks / blocks1.size()) * Math.log10((double)totalBlocks / blocks2.size());
		 case "JS":
			 double commonBlocksJS = getNoOfCommonBlocks(blocks1, blocks2);
			 if (commonBlocksJS < 0) {
				 return commonBlocksJS;
			 }
		 	 return  commonBlocksJS / (blocks1.size() + blocks2.size() - commonBlocksJS);
		 case "EJS":
			 List<Integer> actualBlocksE1 = blocks1.subList(0, blocks1.size()-1); //the last value is the cardinality of e1
			 List<Integer> actualBlocksE2 = blocks2.subList(0, blocks2.size()-1); //the last value is the cardinality of e2
			 int e1Comparisons = blocks1.get(blocks1.size()-1);
			 int e2Comparisons = blocks2.get(blocks2.size()-1);
			 double commonBlocksEJS = getNoOfCommonBlocks(actualBlocksE1, actualBlocksE2);
		 	 if (commonBlocksEJS < 0) {
		 		 return commonBlocksEJS;
		 	 }
		 	 double probability = commonBlocksEJS / (actualBlocksE1.size() + actualBlocksE2.size() - commonBlocksEJS);
		 	 return probability * Math.log10((double)validComparisons / e1Comparisons) * Math.log10((double)validComparisons / e2Comparisons);
		 }
		 return -1;
	}
	
	
	
	public static double getWeight(int blockIndex, int[] blocks1, int[] blocks2, String weightingScheme) {
		return getWeight(blockIndex,blocks1,blocks2,weightingScheme,0,0);
	}
	
	public static double getWeight(int blockIndex, int[] blocks1, int[] blocks2, String weightingScheme, int totalBlocks) {
		return getWeight(blockIndex,blocks1,blocks2,weightingScheme,totalBlocks,0);		
	}
	
	public static double getWeight(int blockIndex, int[] blocks1, int[] blocks2, String weightingScheme, int totalBlocks, long validComparisons) {
		int commonBlocks = 0;
        int noOfBlocks1 = blocks1.length;
        int noOfBlocks2 = blocks2.length;
        for (int i = 0; i < noOfBlocks1; i++) {
            for (int j = 0; j < noOfBlocks2; j++) {
                if (blocks2[j] < blocks1[i]) {
                    continue;
                }

                if (blocks1[i] < blocks2[j]) {
                    break;
                }

                if (blocks1[i] == blocks2[j]) {
					if (commonBlocks == 0 && blocks1[i] != blockIndex) {
						return -1; //comparison has been already performed
					}
                    commonBlocks++;
                }
            }
        }
	
		switch (weightingScheme) {
		 case "CBS":
			 return commonBlocks;
		 case "ECBS":
		 	 return commonBlocks * Math.log10((double)totalBlocks / noOfBlocks1) * Math.log10((double)totalBlocks / noOfBlocks2);
		 case "JS":
		 	 return  ((double)commonBlocks) / (noOfBlocks1 + noOfBlocks2 - commonBlocks);
		 case "EJS":
			 Integer[] actualBlocksE1 = new Integer[blocks1.length-1]; 
			 for (int i=0; i<blocks1.length-1; ++i){
				 actualBlocksE1[i] = Integer.valueOf(blocks1[i]);
			 }
			 int[] actualBlocksE2 = new int[blocks2.length-1]; 
			 System.arraycopy(blocks2, 0, actualBlocksE2, 0, blocks2.length-1);
			 int e1Comparisons = blocks1[blocks1.length-1];
			 int e2Comparisons = blocks2[blocks2.length-1];
			 double commonBlocksEJS = getNoOfCommonBlocks(actualBlocksE1,actualBlocksE2);
			 if (commonBlocksEJS < 0) {
		 		 return commonBlocksEJS;
		 	 }
			 double probability = commonBlocksEJS / (actualBlocksE1.length + actualBlocksE2.length - commonBlocksEJS);
			 return probability * Math.log10((double)validComparisons / e1Comparisons) * Math.log10((double)validComparisons / e2Comparisons);
		 case "ARCS":						 
			 final int[] commonIndices = getCommonBlockIndicesARCS(blocks1, blocks2);
			 if (commonIndices == null) {
				 return -1;
			 }
			 double totalWeight = 0;
			 for (int i = 0; i < commonIndices.length; ++i) {
				 totalWeight += 1.0 / commonIndices[i];
			 }
			 return totalWeight;
		 default:
			 return -2; //error: unsupported weighting scheme
		}
	}
	
	
	 
	

	private static double getNoOfCommonBlocks(Integer[] blocks1, int[] blocks2) {
		double counter = 0.0;		
		Set<Integer> blocks1Set = new HashSet<>();
		Collections.addAll(blocks1Set,blocks1);			
		for (int block2 : blocks2) {
			if (blocks1Set.contains(block2)) {
				counter++;
			}
		}
		return counter;
	}
	
	/**
	 * finds the common blocks of the two block collections and
	 * returns an array of block sizes for the common blocks
	 * @param blocks1 the blocks to which e1 belongs, along with their size [block1,|block1|,block2,|block2|,block3,...]
	 * @param blocks2 the blocks to which e2 belongs, along with their size [block1,|block1|,block2,|block2|,block3,...]
	 * @return an array of block sizes for the common blocks
	 */
	private static int[] getCommonBlockIndicesARCS(int[] blocks1, int[] blocks2) {
		List<Integer> indicesOfCommonBlocks = new ArrayList<>();
		
		Set<Integer> blocks1Set = new HashSet<>();
		for (int i = 0; i < blocks1.length; i+=2) { //store blocks1 in a set
			blocks1Set.add(blocks1[i]);
		}
		for (int i = 0; i < blocks2.length; i+=2) { //find the indices (in blocks2) of common blocks
			if (blocks1Set.contains(blocks2[i])) {
				indicesOfCommonBlocks.add(i);
			}
		}
		int[] result = new int[indicesOfCommonBlocks.size()];
		for (int i = 0; i < result.length; ++i) {
			result[i] = blocks2[indicesOfCommonBlocks.get(i)+1];//store the size of each common block in results
		}
		return result;
	}	
		

	private static Map<Integer, Integer> getCommonBlockIndicesARCS(List<Integer> blocks1, List<Integer> blocks2) {		
		Map<Integer,Integer> newBlocks1 = new HashMap<>();
		for (int i = 0; i < blocks1.size(); i+=2) {
			newBlocks1.put(blocks1.get(i), blocks1.get(i+1));
		}
		Map<Integer,Integer> newBlocks2 = new HashMap<>();
		for (int i = 0; i < blocks2.size(); i+=2) {
			newBlocks2.put(blocks2.get(i), blocks2.get(i+1));
		}		
		newBlocks1.entrySet().retainAll(newBlocks2.entrySet());
		return newBlocks1;
	}

	public static Set<Integer> getCommonBlockIndices(List<Integer> blocks1, List<Integer> blocks2) {
		Set<Integer> intersection = new HashSet<>(blocks1);
		intersection.retainAll(blocks2);
		return intersection;
		
		/*List<Integer> blocks1 = entityIndex.get(e1);
		List<Integer> blocks2 = entityIndex.get(e2);
		boolean firstCommonIndex = false;
		int noOfBlocks1 = blocks1.size();
		int noOfBlocks2 = blocks2.size();
		final List<Integer> indices = new ArrayList<Integer>();
		for (int i = 0; i < noOfBlocks1; i++) {
			for (int j = 0; j < noOfBlocks2; j++) {
				if (blocks2.get(j) < blocks1.get(i)) {
					continue;
				}
				if (blocks1.get(i) < blocks2.get(j)) {
					break;
				}
				if (blocks1.get(i) == blocks2.get(j)) {
					if (!firstCommonIndex) {
						firstCommonIndex = true;
						if (blocks1.get(i) != blockIndex) {
							return null;
						}
					}
					indices.add(blocks1.get(i));
				}
			}
		}
		return indices;*/
	}
	
	public static int getNoOfCommonBlocks(List<Integer> blocks1, List<Integer> blocks2) {
		
		return getCommonBlockIndices(blocks1, blocks2).size();
		
        /*
        boolean firstCommonIndex = false;
        int commonBlocks = 0;
        int noOfBlocks1 = blocks1.size();
        int noOfBlocks2 = blocks2.size();
        for (int i = 0; i < noOfBlocks1; i++) {
        	int block1i = blocks1.get(i);
            for (int j = 0; j < noOfBlocks2; j++) {
            	int blocks2j = blocks2.get(j);
                if (blocks2j < block1i) {
                    continue;
                }

                if (block1i < blocks2j) {
                    break;
                }

                if (block1i == blocks2j) {
                    commonBlocks++;
                    if (!firstCommonIndex) {
                        firstCommonIndex = true;
                        if (block1i != blockIndex) {
                            return -1;
                        }
                    }
                }
            }
        }
        
        return commonBlocks;
        */
    }
	
	/*public static int getNoOfEntityBlocks(Map<Integer,List<Integer>> entityIndex, int entityId) {
		List<Integer> entityBlocks;
        if ((entityBlocks = entityIndex.get(entityId)) == null) {
            return -1;
        }
        
        return entityBlocks.size();
    }*/
	
	public static void main (String[] args) {		
		int [] blocks1 = new int[]{1,2, 2,3, 5,3, 6,5};
		int [] blocks2 = new int[]{1,2, 5,3, 4,5};
		System.out.println(Arrays.toString(getCommonBlockIndicesARCS(blocks1, blocks2)));
	}

}
