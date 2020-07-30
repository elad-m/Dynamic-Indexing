import dynamic_index.global_tools.PrintingTool;
import dynamic_index.index_experiments.LogMergeExperiment;
import dynamic_index.index_experiments.SimpleMergeExperiment;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

@SuppressWarnings({"SameParameterValue"})
public class Main {


    public static void main(String[] args) {
        final String localDir = System.getProperty("user.dir");

//        createInput(localDir, 0.4f, 4);
//        example(localDir);

        System.out.println("======================================= Starting Experiment =======================================");

        SimpleMergeExperiment simpleMergeExperiment =
                new SimpleMergeExperiment(localDir, 5, false);
        simpleMergeExperiment.runExperiment();

        testTempSize(localDir,5,false);
//        LogMergeExperiment logMergeExperiment =
//                new LogMergeExperiment(localDir, 5, 32768, false);
//        logMergeExperiment.runExperiment();
//

    }

    private static void example(String currDir) {
        TreeMap<Integer, String> map = new TreeMap<>();
        map.put(1, null);
        List<String> list = new ArrayList<>();
        list.add(map.get(1));
        PrintingTool.printList(list);
    }

    private static void createInput(String dir, float reviewSplit, int inputScale) {
        try {
            final int LINES_PER_REVIEW = 11;
            File inputFolder = new File(dir + File.separator + "inputFiles");
            Files.createDirectory(inputFolder.toPath());

            String mainFileName = "constructE" + inputScale + ".txt";
            File mainFile = new File(inputFolder.getAbsolutePath() + File.separator + mainFileName);
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

            final int NUM_OF_REVIEW_PER_AUX = 25;
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
        int[] tempIndexSizes = {1024, 1024, 8192, 32768, 131072, 1048576, 4194304, 4 * 4194304};
//        int[] tempIndexSizes = {1048576};
        LogMergeExperiment logMergeExperiment;
        for (int tempSize : tempIndexSizes) {
            logMergeExperiment = new LogMergeExperiment(localDir, inputScale, tempSize, shouldVerify);
            logMergeExperiment.runExperiment();
        }
    }

}

