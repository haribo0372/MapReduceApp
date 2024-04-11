package com.osipov.mapreduce_app;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordSearchReducer extends Reducer<Text, IntWritable, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        StringBuilder articles = new StringBuilder();
        articles.append("[");
        Iterator<IntWritable> iterator = values.iterator();

        if (iterator.hasNext()) articles.append(iterator.next());

        while (iterator.hasNext()) {
            articles.append(", ").append(iterator.next());
        }
        articles.append("]");
        context.write(key, new Text(articles.toString()));
    }
}
