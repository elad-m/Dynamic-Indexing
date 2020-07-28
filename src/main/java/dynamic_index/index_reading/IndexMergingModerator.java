package dynamic_index.index_reading;

import dynamic_index.index_structure.InvertedIndex;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Queues all the SingleIndexReader of all indexes to return each time the next minimum word and its index.
 */
public class IndexMergingModerator {

    private final List<SingleIndexReaderQueue> singleIndexReaderQueues = new ArrayList<>();

    /**
     * Adds a singleIndexReaderQueue to the list of queues.
     * @param singleIndexReader - needed to create the single index reader queue.
     */
    public void add(SingleIndexReader singleIndexReader) {
        try {
            singleIndexReaderQueues.add(new SingleIndexReaderQueue(singleIndexReader));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Creating the index queues in a bulk
     * @param singleIndexReaderList - list of single index readers.
     */
    public void addAll(List<SingleIndexReader> singleIndexReaderList) {
        for (SingleIndexReader singleIndexReader : singleIndexReaderList) {
            add(singleIndexReader);
        }
    }

    /**
     * @return - returns the minimal word from all indexes for merging.
     * Comparison is done by word, and then by the first inverted index value.
     */
    public Map.Entry<String, InvertedIndex> getNextMergingWordAndIndex() {
        Map.Entry<String, InvertedIndex> currMinPair;
        SingleIndexReaderQueue minimumQueue = getMinimumQueue();
        if (minimumQueue == null) {
            currMinPair = null;
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
            for (SingleIndexReaderQueue currentQueue : singleIndexReaderQueues) {
                if (currentQueue.isQueueNotDone()) {
                    Map.Entry<String, InvertedIndex> minPairForCurrentQueue = currentQueue.peek();
                    assert minPairForCurrentQueue != null;
                    Map.Entry<String, InvertedIndex> prevMinPair = minQueue.peek();
                    if(compareWordAndInvertedIndexEntries(minPairForCurrentQueue, prevMinPair) < 0){
                        minQueue = currentQueue;
                    }
                }
            }
            return minQueue;
        }
    }

    private int compareWordAndInvertedIndexEntries(Map.Entry<String, InvertedIndex> entry1,
                                                   Map.Entry<String, InvertedIndex> entry2){
        if(entry1.getKey().compareTo(entry2.getKey()) < 0){
            return -1;
        } else if (entry1.getKey().compareTo(entry2.getKey()) > 0){
            return 1;
        } else { // the same words, compare by inverted index
            return entry1.getValue().compareTo(entry2.getValue());
        }
    }

    private SingleIndexReaderQueue getFirstNotDoneQueue() {
        for (SingleIndexReaderQueue queue : singleIndexReaderQueues) {
            if (queue.isQueueNotDone()) {
                return queue;
            }
        }
        return null; // essentially finished with all the index merging
    }

}
