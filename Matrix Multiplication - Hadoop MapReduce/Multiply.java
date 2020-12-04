/*************************** PSEUDOCODE ******************************
class Elem extends Writable {
  short tag;  // 0 for M, 1 for N
  int index;  // one of the indexes (the other is used as a key)
  double value;
  ...
}

class Pair extends WritableComparable<Pair> {
  int i;
  int j;
  ...
}
First Map-Reduce job:
map(key,line) =             // mapper for matrix M
  split line into 3 values: i, j, and v
  emit(j,new Elem(0,i,v))

map(key,line) =             // mapper for matrix N
  split line into 3 values: i, j, and v
  emit(i,new Elem(1,j,v))

reduce(index,values) =
  A = all v in values with v.tag==0
  B = all v in values with v.tag==1
  for a in A
     for b in B
         emit(new Pair(a.index,b.index),a.value*b.value)
Second Map-Reduce job:
map(key,value) =  // do nothing
  emit(key,value)

reduce(pair,values) =  // do the summation
  m = 0
  for v in values
    m = m+v
  emit(pair,pair.i+","+pair.j+","+m)
*/


package edu.uta.cse6331;
import java.util.*;
import java.io.*;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class MElem implements  Writable {
    public int index;  
    public double value;

    MElem () {}

    MElem (int i, double v ) {
        index = i; value = v;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(index);
        out.writeDouble(value);
    }

    public void readFields ( DataInput in ) throws IOException {
        index = in.readInt();
        value = in.readDouble();
    }
}

class NElem implements  Writable {
    public int index;  
    public double value;

    NElem () {}

    NElem (int i, double v ) {
        index = i; value = v;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(index);
        out.writeDouble(value);
    }

    public void readFields ( DataInput in ) throws IOException {
        index = in.readInt();
        value = in.readDouble();
    }
}

class Elem implements Writable {
    public short tag;
    public MElem m;
    public NElem n;

    Elem () {}
    Elem ( MElem mm ) { tag = 0; m = mm; }
    Elem ( NElem nn ) { tag = 1; n = nn; }

    public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
        if (tag==0)
            m.write(out);
        else n.write(out);
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readShort();
        if (tag==0) {
            m = new MElem();
            m.readFields(in);
        } else {
            n = new NElem();
            n.readFields(in);
        }
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
            MElem e = new MElem(i, v);
            context.write(new IntWritable(j), new Elem(e));
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
            NElem e = new NElem(j, v);
            context.write(new IntWritable(i), new Elem(e));
            s.close();
        }
    }

    public static class MNReducer extends Reducer<IntWritable,Elem, PairIdx, DoubleWritable> {
        static Vector<MElem> m = new Vector<MElem>();
        static Vector<NElem> n = new Vector<NElem>();
        @Override
        public void reduce ( IntWritable key, Iterable<Elem> values, Context context )
                           throws IOException, InterruptedException {
            m.clear();
            n.clear();

            for(Elem v: values) {
                if(v.tag == 0)
                    m.add(v.m);
                else 
                    n.add(v.n);
            }

            for(MElem v1: m)
                for(NElem v2: n)
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
