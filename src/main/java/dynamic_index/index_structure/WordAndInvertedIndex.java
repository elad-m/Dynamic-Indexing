package dynamic_index.index_structure;

/**
 * A Word and its inverted index as byte array (already compressed)
 */
public class WordAndInvertedIndex implements Comparable<WordAndInvertedIndex>{

    private final String word;
    private final InvertedIndexOfWord invertedIndex;


    public WordAndInvertedIndex(String word, InvertedIndexOfWord invertedIndex) {
        this.word = word;
        this.invertedIndex = invertedIndex;
    }

    public String getWord() {
        return word;
    }

    public InvertedIndexOfWord getInvertedIndex() {
        return invertedIndex;
    }

    public int getInvertedIndexLength(){
        return invertedIndex.getNumberOfBytesWrittenToOutput();
    }

    @Override
    public int compareTo(WordAndInvertedIndex other) {
        return this.word.compareTo(other.word);
    }

}
