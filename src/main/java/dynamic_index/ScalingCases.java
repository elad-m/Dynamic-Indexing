package dynamic_index;

@SuppressWarnings("SpellCheckingInspection")
public class ScalingCases {

    final String[] e1InQueries = {"friend", "0", "y"};
    final String[] e1NotInQuery = {"kookie", "storebaught", "nonesenseeee","woordss"};

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

    final String[] we6 = {"0", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
            , "kneepads", "despictable", "rizopoulos", "a"};
    final String[] nwe6 = {"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "kneepadse", "despictableee", "rizopoulosaaa"};
    final String[] pe6 = {"000500005X", "B0002PWL6O", "B009UXKK0S"};
    final String[] npe6 = {"0005000050", "B0002PWL6Z", "B009UXKK0Z"};
    final int[] re6 = {1, 999999, 500000};
    final int[] nre6 = {1000001, 0, -2};

    final String[] we7 = {"0", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
            , "initiative"};
    final String[] nwe7 = {"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            "initiativeeee", "rizopoulosaaa"};
    final String[] pe7 = {"000107461X", "B009VIY9RM", "B0001XAKJ2"};
    final String[] npe7 = {"000107461Z", "B009VIY9RZ", "B0001XAKJZ"};
    final int[] re7 = {1, 7850072, 3000000};
    final int[] nre7 = {7850073, 0, -2};

    static final String MOVIE_REVIEWS1 = "moviesE1.txt";
    static final String MOVIE_REVIEWS2 = "moviesE2.txt";
    static final String MOVIE_REVIEWS3 = "moviesE3.txt";
    static final String MOVIE_REVIEWS4 = "moviesE4.txt";
    static final String MOVIE_REVIEWS5 = "moviesE5.txt";
    static final String MOVIE_REVIEWS6 = "moviesE6.txt";
    static final String MOVIE_REVIEWS7 = "MoviesAndTV.txt";


    private String[] wordQueries = we3;
    private String[] pidQueries = pe3;
    private int[] ridQueries = re3;

    private String[] negWordQueries = nwe3;
    private String[] negPidQueries = npe3;
    private int[] negRidQueries = nre3;

    final private int testType;
    final private String inputFilename;

    public ScalingCases(int eType) {
        testType = eType;
        switch (eType) {
            case 1:
                wordQueries = e1InQueries;
                negWordQueries = e1NotInQuery;
                inputFilename = MOVIE_REVIEWS1;
                break;
            case 2:
                wordQueries = e2InQueries;
                negWordQueries = e2NotInQuery;
                inputFilename = MOVIE_REVIEWS2;
                break;
            case 3:
                wordQueries = we3;
                pidQueries = pe3;
                ridQueries = re3;
                negWordQueries = nwe3;
                negPidQueries = npe3;
                negRidQueries = nre3;
                inputFilename = MOVIE_REVIEWS3;
                break;
            case 4:
                wordQueries = we4;
                pidQueries = pe4;
                ridQueries = re4;
                negWordQueries = nwe4;
                negPidQueries = npe4;
                negRidQueries = nre4;
                inputFilename = MOVIE_REVIEWS4;
                break;
            case 5:
                wordQueries = we5;
                pidQueries = pe5;
                ridQueries = re5;
                negWordQueries = nwe5;
                negPidQueries = npe5;
                negRidQueries = nre5;
                inputFilename = MOVIE_REVIEWS5;
                break;
            case 6:
                wordQueries = we6;
                pidQueries = pe6;
                ridQueries = re6;
                negWordQueries = nwe6;
                negPidQueries = npe6;
                negRidQueries = nre6;
                inputFilename = MOVIE_REVIEWS6;
                break;
            case 7:
                wordQueries = we7;
                pidQueries = pe7;
                ridQueries = re7;
                negWordQueries = nwe7;
                negPidQueries = npe7;
                negRidQueries = nre7;
                inputFilename = MOVIE_REVIEWS7;
                break;
            default:
                inputFilename = "";
                System.err.println("check test input again");
                break;
        }
    }

    public String[] getWordQueries() {
        return wordQueries;
    }

    public String[] getPidQueries() {
        return pidQueries;
    }

    public int[] getRidQueries() {
        return ridQueries;
    }

    public String[] getNegWordQueries() {
        return negWordQueries;
    }

    public String[] getNegPidQueries() {
        return negPidQueries;
    }

    public int[] getNegRidQueries() {
        return negRidQueries;
    }

    public int getTestType() {
        return testType;
    }

    public String getInputFilename() {
        return inputFilename;
    }
}



