package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text characterWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(line.contains(":")) 
        {
            int splitIndex = line.indexOf(':');
            String character = line.substring(0, splitIndex).trim();
            String dialogue = line.substring(splitIndex + 1).trim();

            // Tokenize the dialogue to get individual words
            StringTokenizer tokenizer = new StringTokenizer(dialogue);
            while(tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase(); // Normalize words
                if(!token.isEmpty()) {
                    // Create a combined key of character and word
                    characterWord.set(character + " " + token);
                    context.write(characterWord, one);
                }
            }
        }
    }
}
