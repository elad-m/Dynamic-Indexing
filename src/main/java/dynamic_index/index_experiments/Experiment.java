package dynamic_index.index_experiments;

import dynamic_index.IndexRemover;
import dynamic_index.ScalingCases;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Enumeration;


abstract public class Experiment {

    final String AUXILIARY_INDEX_DIR_PATTERN = "aux";
    public PrintWriter tlog = null;
    public int inputScale = 1;
    final String allIndexesDirectory;
    final String indexParentDirectory;
    final ScalingCases scalingCases;

    public Experiment(String localDir, String indexDirectoryName, int inputScale, boolean logMergeType){
        this.indexParentDirectory = localDir;
        this.allIndexesDirectory = indexDirectoryName;
        this.inputScale = inputScale;
        this.scalingCases = new ScalingCases(this.inputScale, logMergeType);
    }



    public abstract void runExperiment();

    protected String getAuxiliaryIndexDirPattern(int num) {
        return allIndexesDirectory + File.separator + AUXILIARY_INDEX_DIR_PATTERN + num;
    }

    private void removeIndex(String indexDirectoryName) {
        IndexRemover indexRemover = new IndexRemover();
        indexRemover.removeAllIndexFiles(indexDirectoryName);
    }

    protected void printEnumeration(Enumeration<?> enumToPrint) {
        while (enumToPrint.hasMoreElements()) {
            tlog.print(enumToPrint.nextElement().toString() + " ");
            tlog.flush();
        }
        tlog.println();
    }

    protected void printDateAndTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        System.out.println(dtf.format(now));
    }


    protected void createTestLog(String experimentType) {
        try {
            File indexDirectoryFile = new File(this.allIndexesDirectory);
            File testLogFile = new File(indexDirectoryFile.getParent() + File.separator + "0TESTLOG.txt");
            tlog = new PrintWriter(new BufferedWriter(new FileWriter(testLogFile, true)), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        tlog.println("=======================================");
        tlog.println(experimentType);
        tlog.println("Log E" + scalingCases.getTestType());
        tlog.println("=======================================");
        logDateAndTime();
    }

    private void logDateAndTime() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        tlog.println(dtf.format(now));
    }

}
