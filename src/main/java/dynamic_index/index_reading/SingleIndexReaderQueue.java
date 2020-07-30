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
    private boolean isQueueDoneLoading; // finished reading from file
    private boolean isFinishedIndexQueue; // done reading AND empty queue

    // words reading
    private final SingleIndexReader singleIndexReader;
    private int bytesPointer = 0;
    private int rowsPointer = 0;

    // inverted index reading
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
            if(!isQueueDoneLoading) {
                load();
            } else {
                setFinishedIndexQueue();
            }
        }
        return wordAndInvertedIndex;
    }

    private void load(){
        try{
            while(wordToInvertedIndexQueue.isEmpty()){
                if(areThereStillMoreWordsToLoad()){
                    loadQueue();
                } else {
                    setFinishedIndexQueue();
                    break;
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    private boolean areThereStillMoreWordsToLoad(){
        return bytesPointer < singleIndexReader.getIndexDictionaryLength();
    }

    private void loadQueue() throws IOException {
        Map<String, TokenMetaData> wordToTokenMetaData = singleIndexReader.getWordsFromRowOfBytes(bytesPointer, rowsPointer);
        for(Map.Entry<String, TokenMetaData> entry: wordToTokenMetaData.entrySet()){
            InvertedIndex invertedIndex = getInvertedIndex(entry);
            if(invertedIndex != null){ // might get no inverted index because of deletion, FILTERED IN THE LINE ABOVE
                String word = entry.getKey();
                wordToInvertedIndexQueue.put(word, invertedIndex);
            }
        }
        rowsPointer++;
        bytesPointer += singleIndexReader.getFRONT_CODE_ROW_SIZE_IN_BYTES();
        if(bytesPointer >= singleIndexReader.getIndexDictionaryLength()){
            setQueueDoneLoading();
        }
    }

    private InvertedIndex getInvertedIndex(Map.Entry<String, TokenMetaData> wordToTokenMetaData) throws IOException {
        /*
        This is done here so we could read continuously the inverted index with a buffer
         */
        int pointerLength = wordToTokenMetaData.getValue().getFreqLength();
        byte[] rowToReadInto = new byte[pointerLength];
        int numOfReadBytes = invertedIndexBIS.read(rowToReadInto);

        // asserting that we didn't skip a postings list and that the number of bytes read is correct.
        if(totalInvertedIndexBytesReadAssertion != wordToTokenMetaData.getValue().getFreqPointer()){
            System.err.println("Assertion that should not assert");
        }
        totalInvertedIndexBytesReadAssertion += numOfReadBytes;
        assert numOfReadBytes == pointerLength;

        TreeMap<Integer, Integer> ridToFrequencyMap = singleIndexReader.getRidToFreqMapFromRawInvertedIndex(rowToReadInto);
        if(ridToFrequencyMap.isEmpty()){ // could be empty, because of deletion. The method above filters deleted rids.
            return null;
        } else {
            return new InvertedIndex(wordToTokenMetaData.getKey(), ridToFrequencyMap, mainIndexDirectory, singleIndexReader.getCurrentIndexDirectory());
        }
    }

    private void setQueueDoneLoading(){
        isQueueDoneLoading = true;
        try {
            invertedIndexBIS.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return singleIndexReader.toString();
    }

    public boolean isNotFinishedIndexQueue() {
        return !isFinishedIndexQueue;
    }

    private void setFinishedIndexQueue() {
        isFinishedIndexQueue = true;
    }
}
