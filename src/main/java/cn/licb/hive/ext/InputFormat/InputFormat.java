package cn.licb.hive.ext.InputFormat;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * 自定义InputFormat，主要为了支持自定义行分隔符。
 * 使用前，请set textinputformat.record.line.delimiter.bytes=0d 0a;来进行设置分隔符，分隔符使用十六进制来设置。以便支持特殊字符
 *
 * @author Chongbo Li at 2021/1/22
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class InputFormat extends FileInputFormat<LongWritable, Text>
        implements JobConfigurable {

    private CompressionCodecFactory compressionCodecs = null;

    public void configure(JobConf conf) {
        compressionCodecs = new CompressionCodecFactory(conf);
    }

    protected boolean isSplitable(FileSystem fs, Path file) {
        final CompressionCodec codec = compressionCodecs.getCodec(file);
        if (null == codec) {
            return true;
        }
        return codec instanceof SplittableCompressionCodec;
    }

    public RecordReader<LongWritable, Text> getRecordReader(
            InputSplit genericSplit, JobConf job,
            Reporter reporter)
            throws IOException {

        reporter.setStatus(genericSplit.toString());
        //需要使用十六进制的方式设置，多字符需用空格隔开，如0x 0a默认为0a
        String delimiterBytes = job.get("textinputformat.record.line.delimiter.bytes", "0a");
        byte[] recordDelimiterBytes = parseBytesFromString(delimiterBytes);
        return new LineRecordReader(job, (FileSplit) genericSplit,
                recordDelimiterBytes);
    }

    public static byte[] parseBytesFromString(String str) {
        String[] arr = str.trim().split(" ");
        byte[] bytes = new byte[arr.length];
        for (int i = 0; i < arr.length; i++) {
            bytes[i] = (byte) Integer.parseInt(arr[i], 16);
        }

        return bytes;
    }
}

