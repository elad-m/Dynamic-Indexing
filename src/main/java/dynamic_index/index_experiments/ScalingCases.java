package dynamic_index.index_experiments;

import dynamic_index.global_tools.MiscTools;
import dynamic_index.global_tools.ParsingTool;

import java.io.File;
import java.util.*;

public class ScalingCases {

    private final String INSERTION_DIR_NAME = "insertions";
    private final String FIRST_BUILD_FILE_NAME = "first_build.txt";
    private final File insertionDirectory;
    private final File[] insertFiles;

    private final HashSet<Integer> alreadyDeletedRids = new HashSet<>();

    public ScalingCases() {
        insertionDirectory = new File(INSERTION_DIR_NAME);
        insertFiles = getInsertFileNames();
    }

    public List<Integer> getRandomRids(int numberOfReviewsToGet, int lower, int upper){
        List<Integer> randomRids = new ArrayList<>();
        for(int i =0; i < numberOfReviewsToGet; i++){
            randomRids.add(MiscTools.getRandomNumber(lower, upper));
        }
        return randomRids;
    }

    public List<Integer> getRandomRidsNoRepetition(int numberOfReviewsToGet,
                                                   int lower,
                                                   int upper){
        List<Integer> randomRids = new ArrayList<>();
        while(randomRids.size() < numberOfReviewsToGet){
            int ridCandidate = MiscTools.getRandomNumber(lower, upper);
            if (!alreadyDeletedRids.contains(ridCandidate)) {
                randomRids.add((ridCandidate));
            }
        }
        alreadyDeletedRids.addAll(randomRids);
        return randomRids;
    }

    private File[] getInsertFileNames() {
        /* next line orders lexicographically which is not what we want */
        File[] files = insertionDirectory.getAbsoluteFile().listFiles(File::isFile);
        /* order the files numerically */
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                int n1 = extractNumber(o1.getName());
                int n2 = extractNumber(o2.getName());
                return n1 - n2;
            }

            private int extractNumber(String fileName) {
                List<String> fileNameAndExtension =
                        ParsingTool.splitByNonAlphaNumeric(fileName);
                if (!fileNameAndExtension.isEmpty()) {
                    return Integer.parseInt(fileNameAndExtension.get(0));
                } else {
                    System.err.println("Bad file name number extracting");
                    return -1;
                }


            }
        });
        return files;
    }


    public String getInputFilename() {
        return FIRST_BUILD_FILE_NAME;
    }

    public String getInsertFileName(int insertNumber) {
        return insertFiles[insertNumber].getAbsolutePath();
    }

    public int getNumberOfInsertionFiles() {
        return insertFiles.length;
    }

}



