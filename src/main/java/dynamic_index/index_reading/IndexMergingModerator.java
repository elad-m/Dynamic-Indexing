package dynamic_index.index_reading;

import dynamic_index.index_structure.InvertedIndex;


import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Queues all the SingleIndexReader of all indexes to return a merged row of index.
 */
public class IndexMergingModerator {

    private final List<SingleIndexReaderQueue> singleIndexReaderQueues = new ArrayList<>();

    public IndexMergingModerator(){

    }


    public void add(SingleIndexReader singleIndexReader){
        try {
            singleIndexReaderQueues.add(new SingleIndexReaderQueue(singleIndexReader));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Map.Entry<String, InvertedIndex> getNextMergingWordAndIndex(){
        Map.Entry<String, InvertedIndex> currMinPair;
        SingleIndexReaderQueue minimumQueue = getMinimumQueue();
        if(minimumQueue == null){
            currMinPair =  null;
        } else {
            currMinPair = minimumQueue.poll(); // actual removal from data-structure;
        }
        return currMinPair;
    }

    private SingleIndexReaderQueue getMinimumQueue() {
        SingleIndexReaderQueue minQueue = getFirstNotDoneQueue();

        if (minQueue == null) { // all files have been read and written to output buffer
            return null;
        } else {
            for (SingleIndexReaderQueue currentQueue: singleIndexReaderQueues){
                if(currentQueue.isQueueNotDone()){
                    String minPairForCurrentQueue = currentQueue.peek();
                    assert minPairForCurrentQueue != null;
                    String prevMinPair = minQueue.peek();
                    if ( minPairForCurrentQueue.compareTo(prevMinPair) < 0){
                        minQueue = currentQueue;
                    }
                }
            }
            return minQueue;
        }
    }

    private SingleIndexReaderQueue getFirstNotDoneQueue() {
        for (SingleIndexReaderQueue queue: singleIndexReaderQueues){
            if(queue.isQueueNotDone()){
                return queue;
            }
        }
        return null; // essentially finished with all the index merging
    }

}
