package dynamic_index.index_structure;

import java.io.BufferedOutputStream;
import java.util.TreeMap;

/**
 * Multiple InvertedIndex of one word, prior to getting merged.
 * When merging several indexes, a word might appear in some of them, so we get several
 * inverted indexes. The class holds all InvertedIndexes objects of a word, and when needed to write
 * it writes from all of them by the order of the first rid.
 *
 * ASSUMPTION - rids are generated in a monotonically increasing manner and are non-recyclable,
 * so they always go up in the same index, so:
 *  1) the first rid of an InvertedIndex object is its lowest
 *  2) if it is the lowest of all other InvertedIndex objects, then all of its other rids are lower then the
 *  others as well.
 *  This assumption establishes the correctness of the loop in writeTo
 */
public class InvertedIndexesToMerge implements WritingMeasurable{

    private final TreeMap<Integer, InvertedIndex> firstRidToInvertedIndex = new TreeMap<>();
    private int amountOfBytesWrittenExternalOutput = 0;

    /**
     * Puts the InvertedIndex object in the map, while its first rid is the key.
     * @param invertedIndex - inverted index to put in the map
     */
    public void put(InvertedIndex invertedIndex){
        firstRidToInvertedIndex.put(invertedIndex.getFirstRid(), invertedIndex);
    }

    /**
     * Writes all the InvertedIndex objects in the class's map to the given Stream. The loop order
     * is the natural order of the map which is by first rid of each inverted.
     * @param invertedOutputStream - write to this stream.
     */
    public void writeTo(BufferedOutputStream invertedOutputStream) {
        int lastRid = 0;
        // writing all rids
        for(InvertedIndex invertedIndex: firstRidToInvertedIndex.values()){
            lastRid = invertedIndex.writeCompressedRidsTo(invertedOutputStream, lastRid);
        }
        //writing all frequencies
        for(InvertedIndex invertedIndex: firstRidToInvertedIndex.values()){
            invertedIndex.writeCompressedFrequenciesTo(invertedOutputStream);
            amountOfBytesWrittenExternalOutput += invertedIndex.getNumberOfBytesWrittenToOutput();
        }
    }

    @Override
    public int getNumberOfBytesWrittenToOutput() {
        return amountOfBytesWrittenExternalOutput;
    }
}
