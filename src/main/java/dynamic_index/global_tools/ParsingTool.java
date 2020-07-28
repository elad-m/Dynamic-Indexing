package dynamic_index.global_tools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static dynamic_index.global_tools.MiscTools.WHITE_SPACE_SEPARATOR;

/**
 * Does String manipulations and parsings.
 */
public class ParsingTool {


    /**
     * Normalizes a String.
     * @param reviewTextLine - line of text to normalize.
     * @return list of no empty strings, all alphanumeric, lower cased BUT WITH long words > 127 chars
     */
    public static List<String> textToNormalizedTokens(String reviewTextLine) {
        List<String> filteredTokens = new ArrayList<>();
//        String[] tokens = reviewTextLine.split("[^a-zA-Z0-9]+"); // alphanumeric
        List<String> tokens = splitByNonAlphaNumeric(reviewTextLine);
        for (String token : tokens) {
            if (!token.equals("")) // no empty
                filteredTokens.add(token.toLowerCase());  // lower case but includes > 127
        }
        Collections.sort(filteredTokens);
        return filteredTokens;
    }

    public static List<String> splitByNonAlphaNumeric(String text){
        List<String> tokens = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        for(char c: text.toCharArray()){
            if(Character.isLetterOrDigit(c)){
                currentToken.append(c);
            } else {
                if(currentToken.length() > 0){
                    tokens.add(currentToken.toString());
                    currentToken.replace(0, currentToken.length(), "");
                }
            }
        }
        if(currentToken.length() > 0)
            tokens.add(currentToken.toString());
        return tokens;
    }

    /**
     * Extract helpfulness numerator and denominator from a ' n/d' text
     * @param value - a ' n/d' String format
     * @return - a string ' n d'.
     */
    public static String extractHelpfulness(String value) {
        String[] splitHelpfulness = value.split("/");
        return splitHelpfulness[0] + WHITE_SPACE_SEPARATOR + splitHelpfulness[1];
    }
}
