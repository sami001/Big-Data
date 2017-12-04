package edu.uta.cse6331;
import java.util.*;
import java.io.*;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Elem implements  Writable {
    public short tag;  // 0 for M, 1 for N
    public int index;  
    public double value;

    Elem () {}

    Elem ( short t, int i, double v ) {
        tag = t; index = i; value = v;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
        out.writeInt(index);
        out.writeDouble(value);
    }

    public void readFields ( DataInput in ) throws IOException {
        tag  = in.readShort();
        index = in.readInt();
        value = in.readDouble();
    }
}

class PairIdx implements  WritableComparable<PairIdx> {
    public int i;
    public int j;

    PairIdx () {}

    PairIdx (int ii, int jj) {
        i = ii; j = jj;
    }

    @Override
    public int compareTo (PairIdx o ) {
        return (i == o.i) ? j-o.j : i-o.i; 
    }
    @Override
    public void write ( DataOutput out ) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
    }
    @Override
    public void readFields ( DataInput in ) throws IOException {
        i = in.readInt();
        j = in.readInt();
    }
    @Override
    public String toString () { return i+" "+j; }
}

public class Multiply {

    public static class MMapper extends  Mapper<Object,Text,IntWritable, Elem> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int i = s.nextInt();
            int j = s.nextInt();
            double v = s.nextDouble();
            short t = 0;
            Elem e = new Elem(t, i, v);
            context.write(new IntWritable(j), e);
            s.close();
        }
    }

    public static class NMapper extends Mapper<Object,Text,IntWritable, Elem > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int i = s.nextInt();
            int j = s.nextInt();
            double v = s.nextDouble();
            short t = 1;
            Elem e = new Elem(t, j, v);
            context.write(new IntWritable(i), e);
            s.close();
        }
    }

    public static class MNReducer extends Reducer<IntWritable,Elem, PairIdx, DoubleWritable> {
        static Vector<Elem> m = new Vector<Elem>();
        static Vector<Elem> n = new Vector<Elem>();
        @Override
        public void reduce ( IntWritable key, Iterable<Elem> values, Context context )
                           throws IOException, InterruptedException {
            m.clear();
            n.clear();

            for(Elem v: values) {
                if(v.tag == 0)
                    m.add(v);
                else 
                    n.add(v);
            }

            for(Elem v1: m)
                for(Elem v2: n)
                    context.write(new PairIdx(v1.index, v2.index), new DoubleWritable(v1.value * v2.value));
                
        }
    }

    public static class FinalMapper extends Mapper<PairIdx,DoubleWritable,PairIdx, DoubleWritable> {
        @Override
        public void map ( PairIdx key, DoubleWritable values, Context context )
                           throws IOException, InterruptedException {

            context.write(key, values);
        }
    }

    public static class FinalReducer extends Reducer<PairIdx,DoubleWritable,PairIdx, Text> {
        @Override
        public void reduce ( PairIdx key, Iterable<DoubleWritable> values, Context context )
                           throws IOException, InterruptedException {

            double sum = 0.0;
            for(DoubleWritable v: values)
                sum += v.get();
            context.write(key, new Text(key.i+"," + key.j + "," + sum));
        }
    }


    public static void main ( String[] args ) throws Exception {
        Job job1 = Job.getInstance();
        job1.setJobName("Multiplication");
        job1.setJarByClass(Multiply.class);
        
        job1.setOutputKeyClass(PairIdx.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Elem.class);
        job1.setReducerClass(MNReducer.class);
    

        MultipleInputs.addInputPath(job1,new Path(args[0]), TextInputFormat.class, MMapper.class);
        MultipleInputs.addInputPath(job1,new Path(args[1]), TextInputFormat.class, NMapper.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
        
        job1.setNumReduceTasks(2);
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance();
        job2.setJobName("Summation of Multiplications");
        job2.setJarByClass(Multiply.class);
        job2.setOutputKeyClass(PairIdx.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapOutputKeyClass(PairIdx.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setMapperClass(FinalMapper.class);
        job2.setReducerClass(FinalReducer.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2,new Path(args[3]));
        job2.waitForCompletion(true);

    }
}
