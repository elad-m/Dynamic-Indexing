package dynamic_index.index_reading;


/**
 * Data about token (word/pid) that is necessary to complete a search for inverted index
 * of a token, i.e. the frequency pointer and this pointer's length in bytes.
 */
class TokenMetaData {

    private final int freqPointer;
    private final int freqLength;
    //the following is only to get meta data by numbering the words in the dictionary

    TokenMetaData(int freqPointer,
                  int freqLength) {
        this.freqPointer = freqPointer;
        this.freqLength = freqLength;
    }

    int getFreqPointer() {
        return freqPointer;
    }

    int getFreqLength() {
        return freqLength;
    }


}

