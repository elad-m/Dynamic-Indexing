package dynamic_index;

import java.io.File;

@SuppressWarnings("SpellCheckingInspection")
public class ScalingCases {

    final String[] e2InQueries = {"friend", "0", "asdfasdf"};
    final String[] e2NotInQuery = {"kookie", "storebaught", "nonesenseeee", "woordss"};
    final String[] we3 = {"0", "zzzzz", "launched", "1", "a", "he", "in", "yourself", "the"};
    final String[] nwe3 = {"kookie", "storebaught", "juli", "launchea", "zzzzzzzzzzzzzz", "zoomings"};

    final String[] e1 = {"0", "cuban", "friend", "for", "newpaperman", "glamor", "y",
            "aaa", "kindred", "romantic", "atmosphere", "hearted", "posh"};
    final String[] inse1 = {"y", "aaa", "kindred", "romantic", "atmosphere", "hearted", "posh"};
    final String[] delWordse1 = {"cuban", "newpaperman", "glamor", "aaa", "friend", "romantic", "posh"};
    final int[] delReviews1 = new int[]{1, 5, 10, 11, 13};
    final int[] metaReview1 = new int[]{-1, 20, 0, 1, 16, 13};

    final String[] we4 = {"zzzzzzzzzzzzzzzzz", "koreaa", "0", "zzzzzzzzzzzzzz", "korea", "1", "a", "the", "africanism", "grotta", "acheerleader",
            "slinkyness", "amazoni", "epesode", "zaius"};
    final String[] inse4 = {"africanism", "grotta", "acheerleader", "slinkyness", "amazoni", "epesode", "zaius"};
    final String[] delWordse4 = {"africanism", "slinkyness", "zaius"};
    final int[] delReviews4 = new int[]{6556, 10776, 11782, 12238, 1, 10, 500, 10000, 12999, 13000, 10090, 11420};
    final int[] metaReview4 = new int[]{-1, 0, 15000, 1, 6556, 10000, 13000};

    final String[] we5 = {"0", "zzzzzzzzzzzzzz", "kohut", "1", "a", "the"};
    final String[] nwe5 = {"zzzzzzzzzzzzzzzzz", "kohuta", "despairse", "silencinge"};
    final String[] pe5 = {"061530091X", "B0000VD11E", "B009B0STTO"};
    final String[] npe5 = {"0615300910", "B0000VD11Z", "B009B0STTP"};
    final int[] re5 = {1, 100000, 50000};
    final int[] nre5 = {100001, 0, -2};

    final String INSERTION_DIR_E1 = "E1TestResources";
    final String INSERTION_DIR_E4 = "E4TestResources";

    static final String MOVIE_REVIEWS_1 = "moviesE1.txt";
    static final String LOG_MOVIE_REVIEWS_1 = "logMoviesE1.txt";
    static final String MOVIE_REVIEWS_2 = "moviesE2.txt";
    static final String MOVIE_REVIEWS_3 = "moviesE3.txt";
    static final String MOVIE_REVIEWS_4 = "moviesE4.txt";
    static final String LOG_MOVIE_REVIEWS_4 = "logMoviesE4.txt";
    static final String MOVIE_REVIEWS_5 = "moviesE5.txt";

    private String[] wordQueries = e1;
    //    private String[] insertQueries = inse1;
//    private String[] delQueries = delWordse1;
    private int[] delReviews = delReviews1;
    private int[] metaRev = metaReview1;

    private final int testType;
    private final String inputFilename;
    private File insertDirectory;
    private final File[] insertFiles;

    public ScalingCases(int eType, boolean logMergType) {
        this.testType = eType;
        switch (eType) {
            case 1:
                wordQueries = e1;
                if (logMergType)
                    inputFilename = LOG_MOVIE_REVIEWS_1;
                else
                    inputFilename = MOVIE_REVIEWS_1;
                delReviews = delReviews1;
                metaRev = metaReview1;
                insertDirectory = new File(INSERTION_DIR_E1);
                insertFiles = getInsertFileNames();
                break;
            case 4:
                wordQueries = we4;
                if (logMergType)
                    inputFilename = LOG_MOVIE_REVIEWS_4;
                else
                    inputFilename = MOVIE_REVIEWS_4;
                delReviews = delReviews4;
                metaRev = metaReview4;
                insertDirectory = new File(INSERTION_DIR_E4);
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

    private File[] getInsertFileNames() {
        return insertDirectory.getAbsoluteFile().listFiles(File::isFile);
    }

    public String[] getWordQueries() {
        return wordQueries;
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

    public int[] getDelReviews() {
        return delReviews;
    }
}



