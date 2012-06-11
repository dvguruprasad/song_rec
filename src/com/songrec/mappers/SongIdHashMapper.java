package com.songrec.mappers;

import com.songrec.utils.HashingX;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SongIdHashMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try{
            String songId = value.toString().split("\t")[1];
            context.write(new IntWritable(HashingX.hash(songId)), new Text(songId));
        }catch(ArrayIndexOutOfBoundsException exception){
            throw new ArrayIndexOutOfBoundsException(value.toString());
        }
    }
}
