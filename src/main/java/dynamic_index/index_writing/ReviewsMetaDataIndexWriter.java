package dynamic_index.index_writing;

import dynamic_index.global_tools.MiscTools;
import dynamic_index.index_structure.ReviewMetaData;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static dynamic_index.global_tools.MiscTools.WHITE_SPACE_SEPARATOR;

/**
 * Holds the ReviewId to its fields mapping, and write it to a designated index
 * file.
 */
public class ReviewsMetaDataIndexWriter {

    public static final int NUM_OF_REVIEW_META_DATA_FIELDS = 6;
    private BufferedOutputStream bosMetaWriter;

    public ReviewsMetaDataIndexWriter(String allIndexDirectory) {
        initializeFiles(allIndexDirectory);
    }


    private void initializeFiles(String allIndexDirectory) {
        File reviewMetaDataFile = new File(allIndexDirectory + File.separator + MiscTools.REVIEW_META_DATA_FILENAME);
        try {
            bosMetaWriter = new BufferedOutputStream(new FileOutputStream(reviewMetaDataFile, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Writes the review meta data as bytes into the inner output stream.
     * @param value - a white space separated 6 values for a review meta data:
     *              rid, pid, helpfulness numerator, helpfulness denominator, score, review length.
     */
    public void writeData(String value) {
        String[] data = value.split(WHITE_SPACE_SEPARATOR);
        if(data.length == NUM_OF_REVIEW_META_DATA_FIELDS){
            ReviewMetaData reviewMetaData = new ReviewMetaData(data);
            try {
                byte[] ba = reviewMetaData.asByteArray();
                bosMetaWriter.write(ba);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.err.format("review meta data not %s%s", NUM_OF_REVIEW_META_DATA_FIELDS, System.lineSeparator());
        }
    }


    public void closeWriter() {
        try {
            bosMetaWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}
