package dynamic_index.index_reading;


import dynamic_index.index_structure.InvertedIndex;


import java.io.*;
import java.util.Map;
import java.util.TreeMap;

class SingleIndexReaderQueue{

    private final File mainIndexDirectory;

    // assumption - no duplication of words in the same index
//    Queue<WordAndInvertedIndex> wordAndInvertedIndexQueue = new PriorityQueue<>();
    TreeMap<String, InvertedIndex> wordToInvertedIndexQueue = new TreeMap<>();
    SingleIndexReader singleIndexReader;
    int bytesPointer = 0;
    int rowsPointer = 0;
    boolean isDoneReadingFile = false;
    private boolean isQueueDone; // finished reading from file AND queue has been emptied
    BufferedInputStream invertedIndexBIS;

    public SingleIndexReaderQueue(SingleIndexReader singleIndexReader) throws FileNotFoundException {
        this.singleIndexReader = singleIndexReader;
        this.mainIndexDirectory = singleIndexReader.mainIndexDirectory;
        invertedIndexBIS = new BufferedInputStream(new FileInputStream(singleIndexReader.getInvertedIndexFile()));
        load();
    }

    public String peek(){
        return wordToInvertedIndexQueue.firstKey();
    }

    public Map.Entry<String, InvertedIndex> poll(){
        Map.Entry<String, InvertedIndex> wordAndInvertedIndex = wordToInvertedIndexQueue.pollFirstEntry();
        if (wordToInvertedIndexQueue.isEmpty()){
            if(!isDoneReadingFile) {
                load();
            } else {
                setQueueDone();
            }
        }
        return wordAndInvertedIndex;
    }

    private void load(){
        // todo: possibly will have performance issues here for the separate readings of inverted index
        try{
            if(bytesPointer < singleIndexReader.getIndexDictionaryLength()){
                Map<String, TokenMetaData> wordToTokenMetaData = singleIndexReader.getWordsFromRowOfBytes(bytesPointer, rowsPointer);
                for(Map.Entry<String, TokenMetaData> entry: wordToTokenMetaData.entrySet()){
                    InvertedIndex invertedIndex = getInvertedIndex(entry);
                    if(invertedIndex != null){ // might get no inverted index because of deletion
                        String word = entry.getKey();
//                        WordAndInvertedIndex wordAndInvertedIndex = new WordAndInvertedIndex(word, invertedIndex);
                        wordToInvertedIndexQueue.put(word, invertedIndex);
                    }
                }
                rowsPointer++;
                bytesPointer += singleIndexReader.getFRONT_CODE_ROW_SIZE_IN_BYTES();
            }
            if(wordToInvertedIndexQueue.isEmpty()){
                setQueueDone();
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    private InvertedIndex getInvertedIndex(Map.Entry<String, TokenMetaData> wordToTokenMetaData) throws IOException {
        TreeMap<Integer, Integer> map = singleIndexReader.getMapFromRawInvertedIndex(wordToTokenMetaData.getValue());
        if(map.isEmpty()){
            return null;
        } else {
            return new InvertedIndex(map, wordToTokenMetaData.getKey(), mainIndexDirectory);
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

}
