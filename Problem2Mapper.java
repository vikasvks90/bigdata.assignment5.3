/**
 * <h1>Problem2Mapper</h1>
 * Mapper program to find the number of people died or survived in each class with their genders and ages
 * This class will take input as (Key,Value) pair from a given file and will
 * produce the output as (Key,Value) pair.
 * */
package mapreduce.assignment5.problem2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Problem2Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text outkey = new Text();
	IntWritable outvalue = new IntWritable(); 
	public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
	  //getting every line of text file
	  String line = value.toString();
	  //split every line based on ',' and store in String array
	  String str[]=line.split(",");
	  //to ensure class, gender and age are present in the file
	  if(str.length>6){
		  //setting class, gender and age seperated by space
		  String people = str[2]+" "+str[4]+" "+str[5];
		  outkey.set(people);
		  outvalue.set(1);
		  context.write(outkey,outvalue);
	  } 
	}
}
