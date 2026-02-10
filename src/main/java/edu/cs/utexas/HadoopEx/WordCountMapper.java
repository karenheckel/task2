package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
				
		String line = value.toString();
		if (line.startsWith("YEAR,"))
			return;

		String[] fields = line.split(",", -1);
		if (fields.length < 12)
			return;

		String airline = fields[4];
		if (airline.isEmpty())
			return;

		String departureDelay = fields[11];
		if (departureDelay.isEmpty())
			return;
		if (departureDelay.equalsIgnoreCase("NA"))
			return;

		long totalDelay;
		try {
			totalDelay = Math.round(Double.parseDouble(departureDelay));
		} catch (NumberFormatException e) {
			return;
		}

		context.write(new Text(airline), new Text(totalDelay + ",1"));
	}
}