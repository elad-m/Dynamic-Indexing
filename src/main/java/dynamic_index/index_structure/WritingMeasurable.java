package dynamic_index.index_structure;

public interface WritingMeasurable {

    /**
     * Should only be called after this class's writing to external stream, where the bytes calculation takes place.
     * @return size of all the inverted index values -  gaps and frequencies - as bytes when encoded by Length-Precoded Varint method.
     */
    int getNumberOfBytesWrittenToOutput();
}
