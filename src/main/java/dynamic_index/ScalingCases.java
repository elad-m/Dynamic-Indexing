package dynamic_index;

import java.io.File;
import java.io.FileFilter;

@SuppressWarnings("SpellCheckingInspection")
public class ScalingCases {

    final String[] e1 = {"friend", "0", "y"};
    final String[] ne1 = {"nonesenseeee","woordss"};
    final String[] insertE1 = {"sset", "kindred", "romantic", "atmosphere", "hearted", "posh"};

    final String[] e2InQueries = {"friend", "0", "asdfasdf"};
    final String[] e2NotInQuery = {"kookie", "storebaught", "nonesenseeee","woordss"};


    final String[] we3 = {"0", "zzzzz", "launched", "1", "a", "he", "in", "yourself", "the"};
    final String[] nwe3 = {"kookie", "storebaught", "juli", "launchea", "zzzzzzzzzzzzzz", "zoomings"};
    final String[] pe3 = {"1932778764", "1590526848", "1890758035", "B0001BMLZA", "B000CQ55AC", "B009B0STTO"}; // find a better middle B0001BMLZA
    final String[] npe3 = {"1590526840", "A009ASDF5", "Z009ASDF5"};
    final int[] re3 = {1, 11, 12, 999, 1000, 10, 32, 522};
    final int[] nre3 = {1001, 0, -2};

    final String[] we4 = {"0", "zzzzzzzzzzzzzz", "korea", "1", "a", "yourself", "the", "out"};
    final String[] nwe4 = {"zzzzzzzzzzzzzzzzz", "koreaa", "conciergen", "skydiven"};
    final String[] pe4 = {"0782006078", "B000AYHEXC", "B009B0STTO", "B003U0C74E"};
    final String[] npe4 = {"0782006070", "B009B0STTP"};
    final int[] re4 = {1, 10000, 32, 500};
    final int[] nre4 = {10001, 0, -2};

    final String[] we5 = {"0", "zzzzzzzzzzzzzz", "kohut", "1", "a", "the"};
    final String[] nwe5 = {"zzzzzzzzzzzzzzzzz", "kohuta", "despairse", "silencinge"};
    final String[] pe5 = {"061530091X", "B0000VD11E", "B009B0STTO"};
    final String[] npe5 = {"0615300910", "B0000VD11Z", "B009B0STTP"};
    final int[] re5 = {1, 100000, 50000};
    final int[] nre5 = {100001, 0, -2};

    static final String INSERT_1_PATTERN = "insertE1.txt.";
    static final String INSERT_2_PATTERN = "insertE2.txt.";
    static final String INSERT_3_PATTERN = "insertE3.txt.";
    static final String INSERT_4_PATTERN = "insertE4.txt.";
    static final String INSERT_5_PATTERN = "insertE5.txt.";

    static final String DELETE_1 = "deleteE1.txt.";
    static final String DELETE_2 = "deleteE2.txt.";
    static final String DELETE_3 = "deleteE3.txt.";
    static final String DELETE_4 = "deleteE4.txt.";
    static final String DELETE_5 = "deleteE5.txt.";

    static final String MOVIE_REVIEWS_1 = "moviesE1.txt";
    static final String MOVIE_REVIEWS_2 = "moviesE2.txt";
    static final String MOVIE_REVIEWS_3 = "moviesE3.txt";
    static final String MOVIE_REVIEWS_4 = "moviesE4.txt";
    static final String MOVIE_REVIEWS_5 = "moviesE5.txt";

    private String[] wordQueries = e1;
    private String[] negWordQueries = ne1;
    private String[] insertQueries = insertE1;

    final private int testType;
    final private String inputFilename;
    private final File insertDirectory;
    final private File[] insertFiles;
    final private String deleteFileName;

    public ScalingCases(int eType, String insertionDirectory) {
        this.insertDirectory = new File(insertionDirectory);
        this.testType = eType;
        switch (eType) {
            case 1:
                wordQueries = e1;
                negWordQueries = ne1;
                inputFilename = MOVIE_REVIEWS_1;
                insertFiles = getInsertFileNames();
                deleteFileName = DELETE_1;
                insertQueries = insertE1;
                break;
            default:
                inputFilename = "";
                insertFiles = new File[0];
                deleteFileName = "";
                System.err.println("check test input again");
                break;
        }
    }

    private File[] getInsertFileNames() {
        return insertDirectory.getAbsoluteFile().listFiles(File::isFile);
    }

    public String[] getWordQueries() {
        return wordQueries;
    }


    public String[] getNegWordQueries() {
        return negWordQueries;
    }

    public int getTestType() {
        return testType;
    }

    public String getInputFilename() {
        return inputFilename;
    }

    public String getInsertFileName(int insertNumber){
        return insertFiles[insertNumber].getAbsolutePath();
    }

    public int getNumberOfInsertionFiles(){
        return insertFiles.length;
    }

    public String getDeleteFileName(){
        return deleteFileName;
    }

    public String[] getInsertQueries() {
        return insertQueries;
    }
}



