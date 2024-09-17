package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable wordCount = new IntWritable();
    private Text character = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Convert the line of text to a string
        String line = value.toString();

        // Split the line into character name and dialogue using the colon as a separator
        if(line.contains(":")) {
            int splitIndex = line.indexOf(':');
            String characterName = line.substring(0, splitIndex).trim();
            String dialogue = line.substring(splitIndex + 1).trim();

            // Tokenize the dialogue to count words
            StringTokenizer tokenizer = new StringTokenizer(dialogue);
            int count = tokenizer.countTokens();  // Count words in the dialogue

            // Set the character name and word count
            character.set(characterName);
            wordCount.set(count);

            // Write out the character name and dialogue word count
            context.write(character, wordCount);
        }
    }
}
