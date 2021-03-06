package dynamic_index.index_experiments;

import dynamic_index.global_tools.MiscTools;
import dynamic_index.global_tools.ParsingTool;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WordsRandomizer {

    private final Map<Integer, String> wordIdToString;
    private final Map<String, Integer> swapped;

    public WordsRandomizer(String allIndexesDirectory){
        File allIndexesDirectoryFile = new File(allIndexesDirectory);
        this.wordIdToString = loadWordsMapFromFileOutSideIndex(allIndexesDirectoryFile);
        swapped = wordIdToString.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    public List<String> getRandomWords(int numOfWords) {
        List<String> randomWords = new ArrayList<>(numOfWords);
        for (int i = 0; i < numOfWords; i++) {
            int randomNum = MiscTools.getRandomNumber(1, wordIdToString.size());
            String string = wordIdToString.get(randomNum);
            if(string == null){
                System.err.println("null value in wordsRandomizer avoided!");
            } else {
                randomWords.add(string);
            }
        }
        return randomWords;
    }

    public int getWordNumber(String word){
        return swapped.getOrDefault(word, -1);
    }

    public List<String> getAllWords() {
        return new ArrayList<>(wordIdToString.values());
    }



    private Map<Integer, String> loadMapFromFile(File fileToLoad){
        Map<Integer, String> loadedMap = new HashMap<>();
        try (BufferedReader mapBufferedReader =
                     new BufferedReader(new FileReader(fileToLoad))) {
            String line = mapBufferedReader.readLine();
            while (line != null) {
                List<String> wordAndId = ParsingTool.splitByNonAlphaNumeric(line);
                assert wordAndId.size() == 2;
                loadedMap.put(Integer.parseInt(wordAndId.get(1)), wordAndId.get(0));
                line = mapBufferedReader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return loadedMap;
    }

    private Map<Integer, String> loadWordsMapFromFileOutSideIndex(File allIndexesDirectory) {
        String wordsFileName = "words.txt";
        File externalMapFile = new File(allIndexesDirectory.getParent() +
                File.separator + wordsFileName);
        return loadMapFromFile(externalMapFile);
    }
}
