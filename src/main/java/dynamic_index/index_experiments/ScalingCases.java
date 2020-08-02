package dynamic_index.index_experiments;

import dynamic_index.global_tools.MiscTools;
import dynamic_index.global_tools.ParsingTool;

import java.io.File;
import java.util.*;

@SuppressWarnings("SpellCheckingInspection")
public class ScalingCases {

    final String[] we4 = {"zzzzzzzzzzzzzzzzz", "0", "zzzzzzzzzzzzzz", "a", "to", "in"};

    final int[] delReviews4 = new int[]{6556, 10776, 11782, 12238, 1, 10, 500, 10000, 12999, 13000, 10090, 11420};
    final int[] metaReview4 = new int[]{-1, 0, 15000, 1, 6556, 10000, 13000};

    final String[] we5 = {"0", "zzzzzzzzzzzzzz", "kohut", "1", "a", "the"};
    final int[] delReviews5 = new int[]{};
    final int[] metaReview5 = {100001, 0, -2, 1, 100000, 50000};

    final String[] we6 = {"0", "zzzzzzzzzzzzzz", "kohut", "1", "out"};
    final String[] inse6 = {};
    final String[] delWords6 = {};
    final int[] delReviews6 = new int[]{};
    final int[] metaReview6 = {1000001, 0, -2, 1, 1000000, 500000};


    final String INSERTION_DIR_E1 = "E1TestResources";
    static final String MOVIE_REVIEWS_4 = "constructE4.txt";
    static final String MOVIE_REVIEWS_5 = "constructE5.txt";
    final String INSERTION_DIR_E4 = "E4TestResources";
    final String INSERTION_DIR_E5 = "E5TestResources";


//    static final String MOVIE_REVIEWS_4 = "moviesE4.txt";
//    static final String MOVIE_REVIEWS_5 = "moviesE5.txt";
    static final String MOVIE_REVIEWS_6 =  "moviesE6.txt";
    static final String LOG_MOVIE_REVIEWS_6 = MOVIE_REVIEWS_6;

    private String[] wordQueries = we4;
    private int[] delReviews = delReviews4;
    private int[] metaRev = metaReview4;

    private final int testType;
    private final String inputFilename;
    private File insertDirectory;
    private final File[] insertFiles;

    private final HashSet<Integer> alreadyDeletedRids = new HashSet<>();

    public ScalingCases(int eType, boolean logMergType) {
        this.testType = eType;
        switch (eType) {
            case 4:
                wordQueries = we4;
//                if (logMergType)
//                    inputFilename = LOG_MOVIE_REVIEWS_4;
//                else
                    inputFilename = MOVIE_REVIEWS_4;
                delReviews = delReviews4;
                metaRev = metaReview4;
                insertDirectory = new File(INSERTION_DIR_E4);
                insertFiles = getInsertFileNames();
                break;
            case 5:
                wordQueries = we5;
//                if (logMergType)
//                    inputFilename = LOG_MOVIE_REVIEWS_5;
//                else
                    inputFilename = MOVIE_REVIEWS_5;
                delReviews = delReviews5;
                metaRev = metaReview5;
                insertDirectory = new File(INSERTION_DIR_E5);
                insertFiles = getInsertFileNames();
                break;
            case 6:
                wordQueries = we6;
//                if (logMergType)
//                    inputFilename = LOG_MOVIE_REVIEWS_6;
//                else
                    inputFilename = MOVIE_REVIEWS_6;
                delReviews = delReviews6;
                metaRev = metaReview6;
                insertDirectory = new File(INSERTION_DIR_E5);
                insertFiles = getInsertFileNames();
                break;

            default:
                inputFilename = "";
                insertFiles = new File[0];
                System.err.println("check test input again");
                break;
        }
    }

    public int[] getMetaRev() {
        return metaRev;
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
        return randomRids;

    }

    private File[] getInsertFileNames() {
        /* next line orders lexicographically which is not what we want */
        File[] files = insertDirectory.getAbsoluteFile().listFiles(File::isFile);
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

    public List<String> getWordQueries() {
        return Arrays.asList(wordQueries);
    }

    public int getTestType() {
        return testType;
    }

    public String getInputFilename() {
        return inputFilename;
    }

    public String getInsertFileName(int insertNumber) {
        return insertFiles[insertNumber].getAbsolutePath();
    }

    public int getNumberOfInsertionFiles() {
        return insertFiles.length;
    }

}



