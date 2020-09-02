package dynamic_index;

import dynamic_index.index_experiments.LogMergeExperiment;
import dynamic_index.index_experiments.SimpleMergeExperiment;

@SuppressWarnings({"SameParameterValue"})
public class Main {


    /**
     * Runs the Simple and Log experiments. The Log is run with different temporary (in-memory) index sizes.
     * <p>
     * There are three inputs for a run of this main: first_build.txt, words.txt and insertions directory.
     * These should just be in the running folder, no actual standard input needed.
     *
     * @param args -
     */
    public static void main(String[] args) {
        /* Getting local folder*/
        final String localDir = System.getProperty("user.dir");

        System.out.println("======================================= Starting Experiment =======================================");

        SimpleMergeExperiment simpleMergeExperiment =
                new SimpleMergeExperiment(localDir);
        simpleMergeExperiment.runExperiment();

        testTempSize(localDir);

        /* Uncomment for a single run of the Log Merge experiment with some temporary index size*/
//        LogMergeExperiment logMergeExperiment =
//                new LogMergeExperiment(localDir, 65536);
//        logMergeExperiment.runExperiment();


    }


    private static void testTempSize(String localDir) {
        //testing temporary index size
        int[] tempIndexSizes = {
                1024,
                8192, 65536, 524288, 4194304
                , 33554432
        };
        LogMergeExperiment logMergeExperiment;
        for (int tempSize : tempIndexSizes) {
            logMergeExperiment = new LogMergeExperiment(localDir, tempSize);
            logMergeExperiment.runExperiment();
            System.gc();
        }
    }

}

