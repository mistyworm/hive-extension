package cn.licb.hive.ext.InputFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.BitSet;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class InputFormatTest {
    private static final Logger LOG =
            LoggerFactory.getLogger(InputFormat.class);

    private static int MAX_LENGTH = 10000;

    private static JobConf defaultConf = new JobConf();
    private static FileSystem localFs = null;

    static {
        try {
            defaultConf.set("fs.defaultFS", "file:///");
            localFs = FileSystem.getLocal(defaultConf);
        } catch (IOException e) {
            throw new RuntimeException("init failure", e);
        }
    }

    private static Path workDir = localFs.makeQualified(new Path(
            System.getProperty("test.build.data", "/tmp"),
            "InputFormat"));

    @Test
    public void testFormat() throws Exception {
        JobConf job = new JobConf(defaultConf);
        Path file = new Path(workDir, "test.txt");

        // A reporter that does nothing
        Reporter reporter = Reporter.NULL;

        int seed = new Random().nextInt();
        LOG.info("seed = " + seed);
        Random random = new Random(seed);

        localFs.delete(workDir, true);
        FileInputFormat.setInputPaths(job, workDir);

        // for a variety of lengths
        for (int length = 0; length < MAX_LENGTH;
             length += random.nextInt(MAX_LENGTH / 10) + 1) {

            LOG.debug("creating; entries = " + length);

            // create a file with length entries
            Writer writer = new OutputStreamWriter(localFs.create(file));
            try {
                for (int i = 0; i < length; i++) {
                    writer.write(Integer.toString(i));
                    writer.write("\n");
                }
            } finally {
                writer.close();
            }

            // try splitting the file in a variety of sizes
            InputFormat format = new InputFormat();
            format.configure(job);
            LongWritable key = new LongWritable();
            Text value = new Text();
            for (int i = 0; i < 3; i++) {
                int numSplits = random.nextInt(MAX_LENGTH / 20) + 1;
                LOG.debug("splitting: requesting = " + numSplits);
                InputSplit[] splits = format.getSplits(job, numSplits);
                LOG.debug("splitting: got =        " + splits.length);

                if (length == 0) {
                    assertEquals(
                            1, splits.length,
                            "Files of length 0 are not returned from FileInputFormat.getSplits()."
                    );
                    assertEquals(0, splits[0].getLength(),
                            "Empty file length == 0"
                    );
                }

                // check each split
                BitSet bits = new BitSet(length);
                for (int j = 0; j < splits.length; j++) {
                    LOG.debug("split[" + j + "]= " + splits[j]);
                    RecordReader<LongWritable, Text> reader =
                            format.getRecordReader(splits[j], job, reporter);
                    try {
                        int count = 0;
                        while (reader.next(key, value)) {
                            int v = Integer.parseInt(value.toString());
                            LOG.debug("read " + v);
                            if (bits.get(v)) {
                                LOG.warn("conflict with " + v +
                                        " in split " + j +
                                        " at position " + reader.getPos());
                            }
                            assertFalse(bits.get(v),
                                    "Key in multiple partitions."
                            );
                            bits.set(v);
                            count++;
                        }
                        LOG.debug("splits[" + j + "]=" + splits[j] + " count=" + count);
                    } finally {
                        reader.close();
                    }
                }
                assertEquals(length, bits.cardinality(),
                        "Some keys in no partition."
                );
            }

        }
    }

    @Test
    public void testFormat2() throws Exception {
        String[] testArray1 = new String[]{
                "a11111,a22222',\"a33333",
                "b11111,b22222,b3\r3333",
                "c11111,c22222,c33333",
                "d11111,d22222,d33333"
        };
        test(testArray1, "\n", "0a");
        String[] testArray2 = new String[]{
                "a11111,a22222',\"a3\n3333",
                "b11111,b22222,b3\r3333",
                "c11111,c22222,c33333",
                "d11111,d22222,d33333"
        };
        test(testArray2, "\r\n", "0d 0a");
    }

    private void test(String[] testArray, String delimiter, String delimiterBytes) throws Exception {
        JobConf job = new JobConf(defaultConf);
        job.set("textinputformat.record.line.delimiter.bytes", delimiterBytes);
        Path file = new Path(workDir, "test.txt");

        // A reporter that does nothing
        Reporter reporter = Reporter.NULL;

        localFs.delete(workDir, true);
        FileInputFormat.setInputPaths(job, workDir);

        // create a file with length entries
        Writer writer = new OutputStreamWriter(localFs.create(file));
        try {
            for (String s : testArray) {
                writer.write(s);
                writer.write(delimiter);
            }
        } finally {
            writer.close();
        }

        int seed = new Random().nextInt();
        LOG.info("seed = " + seed);
        Random random = new Random(seed);

        // try splitting the file in a variety of sizes
        InputFormat format = new InputFormat();
        format.configure(job);
        LongWritable key = new LongWritable();
        Text value = new Text();
        for (int i = 0; i < 3; i++) {
            int count = 0;
            int numSplits = random.nextInt(5) + 1;
            LOG.debug("splitting: requesting = " + numSplits);
            InputSplit[] splits = format.getSplits(job, numSplits);

            // check each split
            for (int j = 0; j < splits.length; j++) {
                LOG.debug("split[" + j + "]= " + splits[j]);
                RecordReader<LongWritable, Text> reader =
                        format.getRecordReader(splits[j], job, reporter);
                try {
                    while (reader.next(key, value)) {
                        LOG.debug("read : " + value.toString());
                        assertEquals(value.toString(), testArray[count]);
                        count++;
                    }
                    LOG.debug("splits[" + j + "]=" + splits[j] + " count=" + count);
                } finally {
                    reader.close();
                }
            }
        }
    }
}