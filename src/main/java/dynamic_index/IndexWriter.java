package dynamic_index;

import java.util.List;

/**
 * Writes an index to a given directory of an input raw data file taken from
 * Stanford Large Network Dataset Collection (http://snap.stanford.edu/data/index.html).
 * Can construct (first build from scratch), insert (add data to index) and delete.
 */
public interface IndexWriter {

    /**
     * Constructs an index using the given input file.
     * @param inputFile - input file in a format specified in class description.
     */
    int construct(String inputFile);

    /**
     * When used simply, builds an auxiliary index of a given product review data in the auxIndexDirectory.
     *
     * In Log-Merge, since merging automatically, the aux directory is ignored and this call is wrapping
     * directly a construct(inputFile) call.
     * @param inputFile         - product review raw data
     * @param auxIndexDirectory - directory for the auxiliary index. Should be inside the
     *                          main index directory.
     * @return current new number of reviews (previously entered + currently entered)
     */
    int insert(String inputFile, String auxIndexDirectory);

    /**
     * Removing reviews from index s.t. when looking for them or words in them will return null or empty Enumeration
     *
     * @param indexDirectory - directory of the main index, aka "indexes"
     * @param ridsToDelete   - review ids to delete. If not in range, will ignore.
     */
    void removeReviews(String indexDirectory, List<Integer> ridsToDelete);

    /**
     * @return number of reviews indexed, including reviews that have been deleted.
     */
    int getNumberOfReviewsIndexed();
}
