package dynamic_index.index_reading;

import dynamic_index.global_tools.IndexInvalidationTool;
import dynamic_index.global_tools.MiscTools;
import dynamic_index.index_structure.ReviewMetaData;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Set;

import static dynamic_index.global_tools.MiscTools.REVIEW_META_DATA_FILENAME;
import static dynamic_index.global_tools.MiscTools.REVIEW_META_DATA_TEMP_FILENAME;

/**
 * Created with each IndexReader object. This means, that this class can assume no change in index data
 * while this object is alive.
 */
public class ReviewsMetaDataIndexReader {

    private File reviewMetaDataFile;
    private final HashMap<Integer, ReviewMetaData> ridToMetaDataMap = new HashMap<>();
    private int totalNumberOfTokens = 0;

    public ReviewsMetaDataIndexReader(File allIndexDirectory) {
        reviewMetaDataFile = new File(allIndexDirectory.getPath()
                + File.separator + MiscTools.REVIEW_META_DATA_FILENAME);
        loadFileToMap();
        Set<Integer> invalidatedRids = IndexInvalidationTool.getInvalidationSet(this.reviewMetaDataFile.getParent());
        ridToMetaDataMap.keySet().removeAll(invalidatedRids);

    }

    private void loadFileToMap() {
        if(reviewMetaDataFile.exists()){
            try {
                BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(reviewMetaDataFile));
                byte[] ridMetaData = new byte[ReviewMetaData.sizeOfBytesArray];
                int numBytesRead = bufferedInputStream.read(ridMetaData);
                while (numBytesRead != -1) {
                    ReviewMetaData reviewMetaData = new ReviewMetaData(ridMetaData);
                    ridToMetaDataMap.put(reviewMetaData.getRid(), reviewMetaData);
                    numBytesRead = bufferedInputStream.read(ridMetaData);
                }
                bufferedInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println(REVIEW_META_DATA_FILENAME + " file does not exists");
        }

    }

    private ReviewMetaData getReviewMetaData(int rid) {
        if (rid <= 0) {
            return null;
        } else return ridToMetaDataMap.getOrDefault(rid, null);
    }

    /**
     * Called when doing an index merging.
     * Writes to a temporary file, deletes the previous file, renames temp to original name and reassigns
     * to data member.
     */
    public void rewriteReviewMetaData() {
        File tempReviewMetaData = writeTempReviewMetaDataFile();
        if(tempReviewMetaData != null){
            try {
                Path tempReviewMetaDataPath = tempReviewMetaData.toPath();
                Files.delete(reviewMetaDataFile.toPath());
                Files.move(tempReviewMetaDataPath, tempReviewMetaDataPath.resolveSibling(REVIEW_META_DATA_FILENAME));
                this.reviewMetaDataFile = tempReviewMetaData;
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.exit(4);
        }
    }

    private File writeTempReviewMetaDataFile(){
        try {
            File tempMetaFile = new File(reviewMetaDataFile.getParentFile().getPath()
                    + File.separator + REVIEW_META_DATA_TEMP_FILENAME);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
                    new FileOutputStream(tempMetaFile));
            for (ReviewMetaData reviewMetaData : ridToMetaDataMap.values()){
                bufferedOutputStream.write(reviewMetaData.asByteArray());
            }
            bufferedOutputStream.close();
            return tempMetaFile;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    public String getProductId(int reviewId) {
        ReviewMetaData reviewMetaData = getReviewMetaData(reviewId);
        if (reviewMetaData != null) {
            return reviewMetaData.getPid();
        } else {
            return null;
        }
    }

    public int getReviewScore(int reviewId) {
        ReviewMetaData reviewMetaData = getReviewMetaData(reviewId);
        if (reviewMetaData != null) {
            return reviewMetaData.getScore();
        } else {
            return -1;
        }
    }


    public int getReviewHelpfulnessNumerator(int reviewId) {
        ReviewMetaData reviewMetaData = getReviewMetaData(reviewId);
        if (reviewMetaData != null) {
            return reviewMetaData.getHelpfulnessNumerator();
        } else {
            return -1;
        }
    }

    public int getReviewHelpfulnessDenominator(int reviewId) {
        ReviewMetaData reviewMetaData = getReviewMetaData(reviewId);
        if (reviewMetaData != null) {
            return reviewMetaData.getHelpfulnessDenominator();
        } else {
            return -1;
        }
    }

    public int getReviewLength(int reviewId) {
        ReviewMetaData reviewMetaData = getReviewMetaData(reviewId);
        if (reviewMetaData != null) {
            return reviewMetaData.getReviewLength();
        } else {
            return -1;
        }
    }

    /**
     * @return the number of reviews in the index minus the deleted ones
     */
    public int getTotalNumberOfReviews() {
        return ridToMetaDataMap.size();
    }

    /**
     * @return - Number of tokens in all reviews except for the deleted ones (which were already taken care of
     * in the constructor)
     */
    public int getTotalNumberOfTokens(){
        if(totalNumberOfTokens == 0){
            int tokenSum = 0;
            for(ReviewMetaData reviewMetaData: ridToMetaDataMap.values()){
                tokenSum += reviewMetaData.getReviewLength();
            }
            this.totalNumberOfTokens = tokenSum;
        }
        return totalNumberOfTokens;
    }

}
