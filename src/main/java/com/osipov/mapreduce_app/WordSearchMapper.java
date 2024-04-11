package com.osipov.mapreduce_app;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class WordSearchMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text text = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String currentWord = value.toString().toLowerCase();

        Path articles = new Path("/data/sample/articles-sample");
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(articles)));
        String line;

        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\\s+", 2);
            parts[0] = parts[0].replaceAll("\\s", "");
            int articleId = Integer.parseInt(parts[0]);
            String articleText = parts[1];
            String[] words = articleText.toLowerCase().split("\\s+");

            for (String w : words) {
                if (w.equals(currentWord)) {
                    text.set(currentWord);
                    context.write(text, new IntWritable(articleId));
                    break;
                }
            }
        }
        reader.close();
    }
}
