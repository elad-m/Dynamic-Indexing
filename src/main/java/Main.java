import dynamic_index.global_tools.MiscTools;
import dynamic_index.index_experiments.LogMergeExperiment;
import dynamic_index.index_experiments.SimpleMergeExperiment;

import java.io.*;
import java.nio.file.Files;

@SuppressWarnings({"SameParameterValue"})
public class Main {


    /**
     * Chooses a scale of input (e.g. 5 == 100,000 reviews), and runs the Simple and Log experiments. The Log is run with
     * different temporary (in-memory) index sizes.
     * <p>
     * I have already supplied all the input needed
     * for 100,000 reviews (first input, insertions and words in 100,000 reviews
     * <p>
     * If the user wants its own output:
     * 1. an input file with the pattern moviesE?.txt where ? is the scale.
     * 2. a words file in a "words" directory with the name pattern of e?.txt (? as above). This is where
     * the words for querying will come from.
     *
     * @param args -
     */
    public static void main(String[] args) {
        /* Getting local folder*/
        final String localDir = System.getProperty("user.dir");

        /* select input scale */
        int inputScale = 5;

        /* This could be uncommented for creating different input sequence. I have already supplied all the input needed
         * for 100,000 reviews (first input, insertions and words in 100,000 reviews

         * Creates a partition of the input. The second argument tells what percent of the reviews would be used
         * in the first build. The insertion files are 25*10^(inputScale-4) reviews each (25 for 4, 250 for 5 and 2500 for 6)
         * The source of data should be named moviesE?.txt where ? is the input scale.
         * The first build input file ends up in localDir with the name constructE?.txt and the insertion files are placed
         * in a new directory named "insertionFiles"  */
//        createInput(localDir, 0.4f, inputScale);

        System.out.println("======================================= Starting Experiment =======================================");

        SimpleMergeExperiment simpleMergeExperiment =
                new SimpleMergeExperiment(localDir, inputScale, false);
        simpleMergeExperiment.runExperiment();

        testTempSize(localDir, inputScale, false);

        /* Uncomment for a single run of the Log Merge experiment with some temporary index size*/
//        LogMergeExperiment logMergeExperiment =
//                new LogMergeExperiment(localDir, 5, 4194304, false);
//        logMergeExperiment.runExperiment();


    }


    private static void createInput(String localDir, float reviewSplit, int inputScale) {
        try {
            final int LINES_PER_REVIEW = 11;
            File inputFolder = new File(localDir + File.separator + MiscTools.INSERTION_FILES_DIRECTORY);
            Files.createDirectory(inputFolder.toPath());

            String mainFileName = "constructE" + inputScale + ".txt";
            File mainFile = new File(localDir + File.separator + mainFileName);
            Files.createFile(mainFile.toPath());
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(mainFile));

            String sourceFileName = "moviesE" + inputScale + ".txt";
            BufferedReader bufferedReader = new BufferedReader(new FileReader(sourceFileName));
            String line = bufferedReader.readLine();

            int lineCounter = 0;
            int stopAt = (int) ((Math.pow(10.0, inputScale) * LINES_PER_REVIEW) * reviewSplit);

            while (line != null && lineCounter < stopAt) { // 40,000 reviews
                bufferedWriter.write(line + System.lineSeparator());
                line = bufferedReader.readLine();
                lineCounter++;
            }
            bufferedWriter.close();

            final int NUM_OF_REVIEW_PER_AUX = 25 * (int) Math.pow(10, inputScale - 4);
            final int STOP_AUX_AT_LINE = NUM_OF_REVIEW_PER_AUX * LINES_PER_REVIEW;
            int auxCounter = 0;
            int auxLineCounter;
            File auxFile;
            BufferedWriter auxWriter;
            while (line != null) {
                auxLineCounter = 0;
                String fileName = (auxCounter) + ".txt";
                auxFile = new File(inputFolder.getAbsolutePath() + File.separator + fileName);
                Files.createFile(auxFile.toPath());
                auxWriter = new BufferedWriter(new FileWriter(auxFile));
                while (auxLineCounter < STOP_AUX_AT_LINE) {
                    auxWriter.write(line + System.lineSeparator());
                    line = bufferedReader.readLine();
                    auxLineCounter++;
                }
                auxWriter.close();
                auxCounter++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void testTempSize(String localDir, int inputScale, boolean shouldVerify) {
        //testing temporary index size
        int[] tempIndexSizes = {
                1024,
                8192, 65536, 524288, 4194304
                , 33554432
        };
        LogMergeExperiment logMergeExperiment;
        for (int tempSize : tempIndexSizes) {
            logMergeExperiment = new LogMergeExperiment(localDir, inputScale, tempSize, shouldVerify);
            logMergeExperiment.runExperiment();
            System.gc();
        }
    }

}

