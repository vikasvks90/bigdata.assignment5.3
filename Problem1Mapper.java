/**
 * <h1>Problem1Mapper</h1>
 * Mapper program to find the average age of males and females who died in the Titanic tragedy
 * This class will take input as (Key,Value) pair from a given file and will
 * produce the output as (Key,Value) pair.
 * */
package mapreduce.assignment5.problem1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public  class Problem1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	//gender will be the output key for mapper
	Text gender = new Text();
	//age will be the output value for mapper
	IntWritable age = new IntWritable();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	   //getting every line of text file	
	   String line = value.toString();
	   //split every line based on ',' and store in String array
	   String str[]=line.split(",");
	   //to ensure gender and age are present in the file
	   if(str.length>6){
		   //5th element of the array is gender
		   gender.set(str[4]);
		   //getting the members who died 
		   if((str[1].equals("1")) ){
			   //checking if this is numeric data or not
			   if(str[5].matches("\\d+")){
				   int i=Integer.parseInt(str[5]);
				   age.set(i);
			   }
		   }
	   }
	   context.write(gender, age);
  }
}
