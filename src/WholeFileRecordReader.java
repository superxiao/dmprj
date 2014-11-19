// Credits to hadoop-course 
// https://code.google.com/p/hadoop-course/source/browse/HadoopSamples/src/main/java/mr/wholeFile/WholeFileRecordReader.java?r=3



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileRecordReader extends
                RecordReader<LongWritable, Text> {

        private FileSplit split;
        private Configuration conf;
        
        private final Text currValue = new Text();
        private boolean fileProcessed = false;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                        throws IOException, InterruptedException {
                this.split = (FileSplit)split;
                this.conf = context.getConfiguration();
        }
        
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {        
                if ( fileProcessed ){
                        return false;
                }
                
                int fileLength = (int)split.getLength();
                byte [] result = new byte[fileLength];
                
                FileSystem  fs = FileSystem.get(conf);
                FSDataInputStream in = null; 
                try {
                        in = fs.open( split.getPath());
                        IOUtils.readFully(in, result, 0, fileLength);
                        currValue.set(result, 0, fileLength);
                        
                } finally {
                        IOUtils.closeStream(in);
                }
                this.fileProcessed = true;
                return true;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException,
                        InterruptedException {
                return new LongWritable(0);
        }

        @Override
        public Text getCurrentValue() throws IOException,
                        InterruptedException {
                return currValue;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
                return 0;
        }

        @Override
        public void close() throws IOException {
                // nothing to close
        }

}