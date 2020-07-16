package dynamic_index.global_tools;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Handles index invalidating file and filtering.
 */
public final class IndexInvalidationTool {

    private static boolean invalidationDirty = false;
    private static final String INVALIDATION_FILENAME = "invalidation.bin";

    //=========================  invalidation method  =====================================//

    /**
     * Whether there has been writing of rids to delete in the invalidation file.
     * This performance a little - not reading a file when it's empty.
     * @return - true, if there are rids written in the invalidation file, false otherwise.
     */
    public static boolean isInvalidationDirty() {
        return invalidationDirty;
    }

    /**
     * When there are rids that have been added to the invalidation file should be set to true.
     * When there has been a merging of ALL index files, should be set to false.
     * @param setTo - boolean value according to above.
     */
    private static void setInvalidationDirty(boolean setTo) {
        invalidationDirty = setTo;
    }

    /**
     * Add an array of rids to the invalidation file, which makes those reviews now considered deleted from
     * the index. The rids are encoded with Length-Precoded Varint (no gaps so no need for order).
     * @param allIndexDirectory - the directory where all the index directories and files are.
     * @param ridsToDelete - rids that would be written to the invalidation file.
     */
    public static void addToInvalidationFile(String allIndexDirectory, int[] ridsToDelete) {
        // encoding, appending
        try {
            File invalidationFile= getInvalidationFile(allIndexDirectory);
            BufferedOutputStream invalidationBOS = new BufferedOutputStream(new FileOutputStream(invalidationFile, true));
            for (int rid : ridsToDelete) {
                byte[] varintedRid = LengthPrecodedVarintCodec.intToCompressedByteArray(rid);
                invalidationBOS.write(varintedRid);
            }
            setInvalidationDirty(true);
            invalidationBOS.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Clears the invalidation file from data, if it exists.
     * @param allIndexesDirectory - the directory where all the index directories and files are.
     */
    public static void emptyInvalidationFile(String allIndexesDirectory){
        try {
            Path invalidationFilePath = getInvalidationFile(allIndexesDirectory).toPath();
            if(Files.exists(invalidationFilePath)){
                Files.delete(getInvalidationFile(allIndexesDirectory).toPath());
                Files.createFile(getInvalidationFile(allIndexesDirectory).toPath());
                setInvalidationDirty(false);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Filters rids from unfilteredResults by the rids in the invalidation file found in allIndexesDirectories.
     * @param allIndexesDirectory - the directory where all the index directories and files are.
     * @param unfilteredResults - rids to frequency, to filter out the entries with deleted rids.
     */
    public static void filterResults(String allIndexesDirectory, TreeMap<Integer, Integer> unfilteredResults) {
        Set<Integer> invalidationSet = getInvalidationSet(allIndexesDirectory);
        for (Iterator<Map.Entry<Integer, Integer>> it = unfilteredResults.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Integer, Integer> entry = it.next();
            int rid = entry.getKey();
            if (invalidationSet.contains(rid))
                it.remove();
        }
    }

    private static File getInvalidationFile(String allIndexDirectory){
        return  new File(allIndexDirectory + File.separator + INVALIDATION_FILENAME);
    }

    /**
     * Returns all rids that has been deleted (invalidated)
     * @param allIndexDirectory - the directory where all the index directories and files are.
     * @return - a set of all rids that has been deleted (invalidated)
     */
    public static Set<Integer> getInvalidationSet(String allIndexDirectory) {
        if(!invalidationDirty){
            return new HashSet<>();
        }
        Set<Integer> orderedSetOfRids = new HashSet<>();
        try {
            File invalidationFile = getInvalidationFile(allIndexDirectory);
            byte[] rids = Files.readAllBytes(invalidationFile.toPath());
            List<Integer> integerList = LengthPrecodedVarintCodec.decodeBytesToIntegers(rids);
            orderedSetOfRids.addAll(integerList);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return orderedSetOfRids;
    }

}
