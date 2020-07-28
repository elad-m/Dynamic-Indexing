package dynamic_index.index_reading;


import dynamic_index.index_structure.InvertedIndex;


import java.io.*;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reads an index as a queue: from lexicographically lowest word to the highest.
 */
class SingleIndexReaderQueue{

    private final File mainIndexDirectory;

    // since the order or insertion is also lexicographic, this tree-map is de facto a queue
    private final TreeMap<String, InvertedIndex> wordToInvertedIndexQueue = new TreeMap<>();

    private final SingleIndexReader singleIndexReader;
    private int bytesPointer = 0;
    private int rowsPointer = 0;
    private boolean isQueueDone; // finished reading from file AND queue has been emptied
    private final BufferedInputStream invertedIndexBIS;
    private int totalInvertedIndexBytesReadAssertion = 0;

    public SingleIndexReaderQueue(SingleIndexReader singleIndexReader) throws FileNotFoundException {
        this.singleIndexReader = singleIndexReader;
        this.mainIndexDirectory = singleIndexReader.mainIndexDirectory;
        invertedIndexBIS = new BufferedInputStream(new FileInputStream(singleIndexReader.getInvertedIndexFile()));
        load();
    }

    public Map.Entry<String,InvertedIndex> peek(){
        return wordToInvertedIndexQueue.firstEntry();
    }

    public Map.Entry<String, InvertedIndex> poll(){
        Map.Entry<String, InvertedIndex> wordAndInvertedIndex = wordToInvertedIndexQueue.pollFirstEntry();
        if (wordToInvertedIndexQueue.isEmpty()){
            if(!isQueueDone) {
                load();
            } else {
                setQueueDone();
            }
        }
        return wordAndInvertedIndex;
    }

    private void load(){
        try{
            if(bytesPointer < singleIndexReader.getIndexDictionaryLength()){
                Map<String, TokenMetaData> wordToTokenMetaData = singleIndexReader.getWordsFromRowOfBytes(bytesPointer, rowsPointer);
                for(Map.Entry<String, TokenMetaData> entry: wordToTokenMetaData.entrySet()){
                    InvertedIndex invertedIndex = getInvertedIndex(entry);
                    if(invertedIndex != null){ // might get no inverted index because of deletion
                        String word = entry.getKey();
                        wordToInvertedIndexQueue.put(word, invertedIndex);
                    }
                }
                rowsPointer++;
                bytesPointer += singleIndexReader.getFRONT_CODE_ROW_SIZE_IN_BYTES();
            }
            // reading from bytesPointer got nothing into the map, so no more entries in index
            if(wordToInvertedIndexQueue.isEmpty()){
                setQueueDone();
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    private InvertedIndex getInvertedIndex(Map.Entry<String, TokenMetaData> wordToTokenMetaData) throws IOException {
        int pointerLength = wordToTokenMetaData.getValue().getFreqLength();
        byte[] rowToReadInto = new byte[pointerLength];
        int numOfReadBytes = invertedIndexBIS.read(rowToReadInto);

        // asserting that we didn't skip a postings list and that the number of bytes read is correct.
        totalInvertedIndexBytesReadAssertion += numOfReadBytes;
        assert totalInvertedIndexBytesReadAssertion == wordToTokenMetaData.getValue().getFreqPointer();
        assert numOfReadBytes == pointerLength;

        TreeMap<Integer, Integer> ridToFrequencyMap = singleIndexReader.getRidToFreqMapFromRawInvertedIndex(rowToReadInto);
        if(ridToFrequencyMap.isEmpty()){ // could be empty, because of deletion. The method above filters deleted rids.
            return null;
        } else {
            return new InvertedIndex(wordToTokenMetaData.getKey(), ridToFrequencyMap, mainIndexDirectory, singleIndexReader.getCurrentIndexDirectory());
        }
    }

    private void setQueueDone(){
        isQueueDone = true;
        try {
            invertedIndexBIS.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isQueueNotDone(){
        return !isQueueDone;
    }

    @Override
    public String toString() {
        return singleIndexReader.toString();
    }
}
