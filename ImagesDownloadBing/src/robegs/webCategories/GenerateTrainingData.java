/** imageDownloader (Bing).
 *
 * Author: Roberto Gonzalez <roberto.gonzalez@neclab.eu>
 *
 * Copyright (c) 2017, NEC Europe Ltd., NEC Corporation. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THIS HEADER MAY NOT BE EXTRACTED OR MODIFIED IN ANY WAY.
 */
package robegs.webCategories;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenerateTrainingData {

    public static void main(String[] args) throws Exception {


        
        
        String hostname = "Unknown";

        try {
            InetAddress addr;
            addr = InetAddress.getLocalHost();
            hostname = addr.getHostName();
        } catch (UnknownHostException ex) {
            System.out.println("Hostname cannot be resolved");
        }

        
        Configuration conf = new Configuration();
        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.set("mapreduce.job.queuename", "alpha");
        conf.setLong("mapreduce.task.timeout", 1000 * 60 * 60);
        conf.setDouble("mapreduce.job.reduce.slowstart.completedmaps", 0.75);
        conf.set("mapred.textoutputformat.separator", "\t");
        // conf.setBoolean("mapred.mapper.new-api",true);

        conf.set("conf.host", hostname);
//
//        conf.addResource("/usr/local/hadoop/etc/hadoop/core-site.xml");
//        conf.addResource("/usr/local/hadoop/etc/hadoop/hdfs-site.xml");

        Job job = new Job(conf, "Collecting Images (Bing)");

//        job.setPartitionerClass(NaturalKeyPartitioner.class);
//        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
//        job.setSortComparatorClass(CompositeKeyComparator.class);
        job.setJarByClass(GenerateTrainingData.class);
        job.setMapperClass(MapperCollector.class);
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(MetaDataReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setOutputFormatClass(HdMultipleFileOutputFormat.class);

        
        System.out.println("----------------------------");
        System.out.println(hostname);
        System.out.println("----------------------------");

        Path outFolder = null;
        if (hostname.equals("robegs-laptop.office.hd")) {
            /*
            Local: For debugging
             */
            
            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, new Path("/mnt/localDisk/imageDownload/id-names-hint_100.txt"));
            outFolder = new Path("/mnt/localDisk/imageDownload/bing/");
        } else {
            /*
            Cluster
             */
            job.setNumReduceTasks(1024);
            FileInputFormat.addInputPath(job, new Path("/user/rgonzalez/downloadImages/missing_id-names.txt"));
//            FileInputFormat.addInputPath(job, new Path("/user/rgonzalez/downloadImages/input/"));
//            outFolder = new Path("/user/rgonzalez/downloadImages/bing/");
            outFolder = new Path("/user/rgonzalez/downloadImages/bingMissing/");
        }

        FileSystem hdfs = FileSystem.get(new Configuration());
        if (hdfs.exists(outFolder)) {
            hdfs.delete(outFolder, true); //Delete existing Directory
        }
        FileOutputFormat.setOutputPath(job, outFolder);
//                
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
