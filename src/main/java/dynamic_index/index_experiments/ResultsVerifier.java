package dynamic_index.index_experiments;

import dynamic_index.global_tools.IndexInvalidationTool;
import dynamic_index.global_tools.ParsingTool;
import dynamic_index.global_tools.PrintingTool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Verifies results of a query. A result of a query is a postings list of both rids and
 * frequencies and will be tested against a file that holds the true results.
 */
public class ResultsVerifier {

    private final String e4VerifyingFile = "indexE4Verifier.txt";

    private final File fileToVerifyWith;
    private final String allIndexesDirectory;

    /**
     * Creates a query result verifier according to an input scale.
     *
     * @param indexParentDir      - where the indexes directory is
     * @param allIndexesDirectory - where the indexes are, to get the invalidation file
     * @param inputScale          - exponential scale of number of reviews: 4 -> 10^4=10,000
     */
    public ResultsVerifier(String indexParentDir, String allIndexesDirectory, int inputScale) {
        this.allIndexesDirectory = allIndexesDirectory;
        switch (inputScale) {
            case 4:
                fileToVerifyWith = new File(indexParentDir + File.separator + e4VerifyingFile);
                break;
            case 5:
                fileToVerifyWith = null;
                break;
            default:
                System.err.println("I don't care");
                fileToVerifyWith = null;
                break;
        }
    }

    /**
     * Verifies that a single query result is correct.
     *
     * @param word              - the word of the query.
     * @param wordNumber        - the word number in the vocabulary held in the WordsRandomizer,
     *                          needed to quickly find the true postings list in the true file.
     * @param actualResult      - result gotten by the experiment.
     * @param reviewNumberSoFar - number of reviews entered by this point, since the true file
     *                          hold results for all reviews
     */
    public void verifySingleQuery(String word,
                                  int wordNumber,
                                  Map<Integer, Integer> actualResult,
                                  int reviewNumberSoFar) {
        if (wordNumber == -1) { // if a word doesn't exists we should get empty list
            assertResultsEmpty(word, actualResult);
        }
        SortedMap<Integer, Integer> trueResult = getVerifiedResult(word, wordNumber, reviewNumberSoFar);
        assertResultsSameLength(word, trueResult, actualResult);

        for (Map.Entry<Integer, Integer> actualEntry: actualResult.entrySet()) {
            int actualRid = actualEntry.getKey();
            int actualFreq = actualEntry.getValue();
            if (areBadResults(word, trueResult, actualRid, actualFreq)) {
                System.err.println("Bad Assertion, actual:");
                PrintingTool.printMap(actualResult);
                break;
            }
        }
    }

    private boolean areBadResults(String word, SortedMap<Integer, Integer> trueResult, int actualRid, int actualeFreq) {
        boolean isBad;
        if(!trueResult.containsKey(actualRid)){
            System.err.format(word + ": " + "actual rid-%d not found in true file.\n", actualRid);
            isBad = true;
        } else {
            int trueFreq = trueResult.get(actualRid);
            if(trueFreq != actualeFreq){
                System.err.format(word + ": " + "actual freq-%d is different than the true freq-%d.\n",
                        actualeFreq,
                        trueFreq);
                isBad = true;
            } else {
                isBad = false;
            }
        }
        return isBad;
    }

    private void assertResultsSameLength(String word,
                                         SortedMap<Integer, Integer> trueRidFreqList,
                                         Map<Integer, Integer> actualResult) {
        if (trueRidFreqList.size() != actualResult.size()) {
            System.err.println(word + ": Results not the same length:");
            PrintingTool.printMap(actualResult);
        }
    }

    private void assertResultsEmpty(String word, Map<Integer, Integer> actualResult) {
        if (actualResult.size() > 0) {
            System.err.println(word + ": Found results although it shouldn't");
        }
    }


    private SortedMap<Integer, Integer> getVerifiedResult(String word, int wordNumber, int reviewNumberSoFar) {
        TreeMap<Integer, Integer> wholeVerifiedPostingsList = getWholePostingsList(word, wordNumber);
        SortedMap<Integer, Integer> verifiedTrimmedToActualNumberOfReviews = trimToActualNumberOfReviews(wholeVerifiedPostingsList, reviewNumberSoFar);
        if (IndexInvalidationTool.isInvalidationDirty()) {
            IndexInvalidationTool.filterResults(allIndexesDirectory
                    , verifiedTrimmedToActualNumberOfReviews);
        }
        return verifiedTrimmedToActualNumberOfReviews;
    }

    private SortedMap<Integer, Integer> trimToActualNumberOfReviews(TreeMap<Integer, Integer> wholeVerifiedPostingsList, int reviewNumberSoFar) {
        return  wholeVerifiedPostingsList.subMap(0, reviewNumberSoFar+1);
    }

    private TreeMap<Integer, Integer> getWholePostingsList(String word, int wordNumber) {
        TreeMap<Integer, Integer> wholeVerifiedPostingsList = new TreeMap<>();
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileToVerifyWith));
            int lineCounter = 0;
            String line = null;
            while (lineCounter < wordNumber) {
                line = bufferedReader.readLine();
                lineCounter++;
            }
            if (line != null) {
                List<String> tokens = ParsingTool.splitByNonAlphaNumeric(line);
                assert tokens.get(0).equals(word);
                tokens.remove(0);
                assert tokens.size() % 2 == 0;

                for (int i = 0; i < tokens.size(); i += 2) {
                    int rid = Integer.parseInt(tokens.get(i));
                    int freq = Integer.parseInt(tokens.get(i + 1));
                    wholeVerifiedPostingsList.put(rid, freq);
                }
            }
            return wholeVerifiedPostingsList;
        } catch (IOException e) {
            e.printStackTrace();
            return wholeVerifiedPostingsList;
        }
    }

}
