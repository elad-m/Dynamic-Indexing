package dynamic_index.index_reading;


import dynamic_index.index_structure.InvertedIndexOfWord;
import dynamic_index.index_structure.WordAndInvertedIndex;

import java.io.*;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;

class SingleIndexReaderQueue{

    private final File mainIndexDirectory;
    Queue<WordAndInvertedIndex> wordAndInvertedIndexQueue = new PriorityQueue<>();
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

    public WordAndInvertedIndex peek(){
        return wordAndInvertedIndexQueue.peek();
    }

    public WordAndInvertedIndex poll(){
        WordAndInvertedIndex wordAndInvertedIndex = wordAndInvertedIndexQueue.poll();
        if (wordAndInvertedIndexQueue.isEmpty()){
            if(!isDoneReadingFile) {
                load();
            } else {
                setQueueDone();
            }
        }
        return wordAndInvertedIndex;
    }

    private void load(){
        // todo: possibly will have performance here for the separate readings of inverted index
        try{
            if(bytesPointer < singleIndexReader.getIndexDictionaryLength()){
                Map<String, TokenMetaData> wordToTokenMetaData = singleIndexReader.getWordsFromRowOfBytes(bytesPointer, rowsPointer);
                for(Map.Entry<String, TokenMetaData> entry: wordToTokenMetaData.entrySet()){
                    InvertedIndexOfWord invertedIndex = getInvertedIndex(entry);
                    if(invertedIndex != null){ // might get no inverted index because of deletion
                        String word = entry.getKey();
                        WordAndInvertedIndex wordAndInvertedIndex = new WordAndInvertedIndex(word, invertedIndex);
                        wordAndInvertedIndexQueue.add(wordAndInvertedIndex);
                    }
                }
                rowsPointer++;
                bytesPointer += singleIndexReader.getFRONT_CODE_ROW_SIZE_IN_BYTES();
            }
            if(wordAndInvertedIndexQueue.isEmpty()){
                setQueueDone();
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    private InvertedIndexOfWord getInvertedIndex(Map.Entry<String, TokenMetaData> wordToTokenMetaData) throws IOException {
        TreeMap<Integer, Integer> map = singleIndexReader.getMapFromRawInvertedIndex(wordToTokenMetaData.getValue());
        if(map.isEmpty()){
            return null;
        } else {
            return new InvertedIndexOfWord(map, wordToTokenMetaData.getKey(), mainIndexDirectory);
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
