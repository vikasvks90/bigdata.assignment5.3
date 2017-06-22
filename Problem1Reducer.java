/**
 * <h1>Problem1Reducer</h1>
 * Reducer program to find the average age of males and females who died in the Titanic tragedy
 * This class will take input as (Key,Value) pair from output of mapper class
 * value will be a combined list for all the values for a given key
 * */
package mapreduce.assignment5.problem1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Problem1Reducer extends Reducer<Text,IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	int sumOfAges = 0;
	int totalPersonsDied = 0;
	for (IntWritable val : values) {
		totalPersonsDied += 1;
		sumOfAges += val.get();
	}
	//getting the average age of total persons died 
	sumOfAges = sumOfAges/totalPersonsDied;
	context.write(key, new IntWritable(sumOfAges));
	}
}
