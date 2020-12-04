/*********************************** PSEUDOCODE ***************************************
class Vertex extends Writable {
  short tag;                 // 0 for a graph vertex, 1 for a group number
  long group;                // the group where this vertex belongs to
  long VID;                  // the vertex ID
  Vector adjacent;     // the vertex neighbors
  ...
}
Vertex must have two constructors: Vertex(tag,group,VID,adjacent) and Vertex(tag,group).
First Map-Reduce job:

map ( key, line ) =
  parse the line to get the vertex VID and the adjacent vector
  emit( VID, new Vertex(0,VID,VID,adjacent) )
Second Map-Reduce job:
map ( key, vertex ) =
  emit( vertex.VID, vertex )   // pass the graph topology
  for n in vertex.adjacent:
     emit( n, new Vertex(1,vertex.group) )  // send the group # to the adjacent vertices

reduce ( vid, values ) =
  m = Long.MAX_VALUE;
  for v in values:
     if v.tag == 0
        then adj = v.adjacent.clone()     // found the vertex with vid
     m = min(m,v.group)
  emit( m, new Vertex(0,m,vid,adj) )      // new group #
Final Map-Reduce job:
map ( group, value ) =
   emit(group,1)

reduce ( group, values ) =
   m = 0
   for v in values
       m = m+v
   emit(group,m)
The second map-reduce job must be repeated multiple times.
*/

package edu.uta.cse6331;
import java.util.*;
import java.io.*;
import java.util.Scanner;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;

import org.apache.hadoop.util.ToolRunner;

class GraphVertex implements  Writable {
    public long group;                // the group where this vertex belongs to
    public long vid;                  // the vertex ID
    public Vector<Long> adjacent  = new Vector<Long>();     // the vertex neighbors

    GraphVertex () {}

    GraphVertex (long g, long v, Vector<Long> a) {
        group = g;
        vid = v;
        adjacent = a;
    }

    @Override
    public void write ( DataOutput out ) throws IOException {
        out.writeLong(group);
        out.writeLong(vid);
       // out.writeVector(adjacent);
        out.writeInt(adjacent.size());
        for (int i =0;i<adjacent.size();i++) {
            out.writeLong(adjacent.get(i));
        }
    }

    @Override
    public void readFields ( DataInput in ) throws IOException {
        group = in.readLong();
        vid = in.readLong();
        int length = in.readInt();

        if (length>0){
            adjacent.clear();

            for (int i = 0; i < length; i++) 
                adjacent.addElement(in.readLong());
        
        }
    }
}

class GroupNumber implements  Writable {
   public long group;                // the group where this vertex belongs to

    GroupNumber () {}

    GroupNumber (long g) {
        group = g;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeLong(group);
    }

    public void readFields ( DataInput in ) throws IOException {
       group = in.readLong();
    }
}

class Vertex  implements Writable {
    public short tag;
    public GraphVertex gv;
    public GroupNumber gn;

    Vertex () {}
    Vertex ( GraphVertex gv ) { tag = 0; this.gv = gv; }
    Vertex ( GroupNumber gn ) { tag = 1; this.gn = gn; }

    public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
        if (tag==0)
            gv.write(out);
        else gn.write(out);
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readShort();
        if (tag==0) {
            gv = new GraphVertex();
            gv.readFields(in);
        } else {
            gn = new GroupNumber();
            gn.readFields(in);
        }
    }
}

public class Graph extends Configured implements Tool {

    public static class Mapper1 extends  Mapper<Object,Text,LongWritable,Vertex> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            long v = s.nextLong();
            Vector<Long> a = new Vector<Long>();
            while(s.hasNextLine()) {
                a.addElement(s.nextLong());
            }
            GraphVertex gv = new GraphVertex(v, v, a);
            context.write(new LongWritable(v), new Vertex(gv));
            s.close();
        }
    }
    /*
    public static class Reducer1 extends Reducer<LongWritable,Vertex,LongWritable, Vertex> {
        @Override
        public void reduce ( LongWritable key, Vertex values, Context context )
                           throws IOException, InterruptedException {

            context.write(key, values);
        }
    }*/

    public static class Mapper2 extends Mapper<LongWritable,Vertex,LongWritable, Vertex > {
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                        throws IOException, InterruptedException {
            if(value.tag == 0) {
                context.write(new LongWritable(value.gv.vid), value);                
                for (long n: value.gv.adjacent){    
                    GroupNumber gn = new GroupNumber(value.gv.group);
                    context.write(new LongWritable(n), new Vertex(gn));
                }
            }
        }
    }
    public static class Reducer2 extends Reducer<LongWritable,Vertex, LongWritable, Vertex> {
        @Override
        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
                           throws IOException, InterruptedException {
            
            long m = Long.MAX_VALUE;
            Vector<Long> adj = new Vector<Long>();

            for(Vertex v: values) {
                if(v.tag == 0) {
                    adj = v.gv.adjacent;
                    if(v.gv.group < m)
                        m = v.gv.group;
                }
                else {
                    if(v.gn.group < m)
                        m = v.gn.group;
                }
            } 

            GraphVertex gv  = new GraphVertex(m, key.get(), adj);
            context.write(new LongWritable(m), new Vertex(gv));               
        }
    }

    public static class Mapper3 extends Mapper<LongWritable, Vertex,LongWritable, LongWritable> {
        @Override
        public void map ( LongWritable key, Vertex values, Context context )
                           throws IOException, InterruptedException {
            System.out.println("Key: "+key.get()+"\t");
          //  for(Vertex v: values)
            System.out.println(values.gv.vid+" ");
          //  System.out.println("\n");



         //   System.out.println("Key: "+key.get()+"\tValue: 1");                    
            context.write(key, new LongWritable(1));
        }
    }
    public static class Reducer3 extends Reducer<LongWritable,LongWritable,LongWritable, LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
                           throws IOException, InterruptedException {

            long sum = 0;
            for(LongWritable v: values)
                sum += v.get();
            context.write(key, new LongWritable(sum));
        }
    }



    @Override
    public int run (String [] args) throws Exception {
     //   Configuration conf = getConf();
        Job job1 = Job.getInstance();
        job1.setJobName("first");
        job1.setJarByClass(Graph.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);
        job1.setMapperClass(Mapper1.class);
       // job1.setReducerClass(Reducer1.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job1,new Path(args[1]+"/f0"));
        job1.setNumReduceTasks(0); // ????????????????????
        job1.waitForCompletion(true);

        for(int i = 0; i < 5; i++){
            Job job2 = Job.getInstance();
            job2.setJobName("second");
            job2.setJarByClass(Graph.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);
            job2.setMapperClass(Mapper2.class);
            job2.setReducerClass(Reducer2.class);
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job2, new Path(args[1]+"/f"+i));
            FileOutputFormat.setOutputPath(job2,new Path(args[1]+"/f"+(i+1)));
            job2.waitForCompletion(true);
        }

        Job job3 = Job.getInstance();
        job3.setJobName("third");
        job3.setJarByClass(Graph.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job3, new Path(args[1]+"/f5"));
        FileOutputFormat.setOutputPath(job3,new Path(args[2]));
        job3.waitForCompletion(true);

        return 0;
    }

    public static void main ( String[] args ) throws Exception {
        int res = ToolRunner.run(new Configuration(),new Graph(), args);
        System.exit(res);    
    }
}
