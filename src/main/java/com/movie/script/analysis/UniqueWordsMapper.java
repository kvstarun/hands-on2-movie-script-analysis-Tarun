package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class UniqueWordsMapper extends Mapper<Object, Text, Text, Text> {

    private Text character = new Text();
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Convert the line of text to a string and split it into character name and dialogue
        String line = value.toString();
        if (line.contains(":")) {
            int splitIndex = line.indexOf(':');
            String characterName = line.substring(0, splitIndex).trim();
            String dialogue = line.substring(splitIndex + 1).trim();

            // Use a HashSet to track unique words in the current line of dialogue
            HashSet<String> uniqueWords = new HashSet<>();
            StringTokenizer tokenizer = new StringTokenizer(dialogue);

            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase(); // Normalize words
                if (!token.isEmpty()) {
                    uniqueWords.add(token);
                }
            }

            // Emit each unique word along with the character name
            character.set(characterName);
            for (String uniqueWord : uniqueWords) {
                word.set(uniqueWord);
                context.write(character, word);
            }
        }
    }
}

