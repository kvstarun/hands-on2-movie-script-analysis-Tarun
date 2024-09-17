package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class UniqueWordsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> uniqueWords = new HashSet<>();

        // Iterate over all words associated with the same character
        for (Text value : values) {
            uniqueWords.add(value.toString()); // Add each word to the set, automatically removing duplicates
        }

        // Construct a comma-separated list of unique words for readability
        StringBuilder uniqueWordsList = new StringBuilder();
        for (String word : uniqueWords) {
            if (uniqueWordsList.length() > 0) {
                uniqueWordsList.append(", ");
            }
            uniqueWordsList.append(word);
        }

        // Emit the character and their unique word list
        context.write(key, new Text(uniqueWordsList.toString()));
    }
}

