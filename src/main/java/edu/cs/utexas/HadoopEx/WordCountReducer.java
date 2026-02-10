package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text text, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long totalDelay = 0L;
        long numFlights = 0L;

        // Each value is totalDelay, numFlights for a particular airline
        for (Text value : values) {
            String[] parts = value.toString().split(",", -1);
            totalDelay += Long.parseLong(parts[0]);
            numFlights += Long.parseLong(parts[1]);
        }

        context.write(text, new Text(totalDelay + "," + numFlights));
    }
}
