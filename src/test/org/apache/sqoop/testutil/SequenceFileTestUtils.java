package org.apache.sqoop.testutil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.util.ClassLoaderStack;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SequenceFileTestUtils {

    private static final String OUTPUT_FILE_NAME = "/part-m-00000";

    public static final Log LOG = LogFactory.getLog(
            SequenceFileTestUtils.class.getName());

    /**
     * Verify results at the given tablePath.
     * @param testCase current instance of BaseSqoopTestCase
     * @param expectedResults string array of expected results
     * @param fileSystem current fileSystem
     * @param tablePath path of the output table
     */
    public static void verify(BaseSqoopTestCase testCase, String[] expectedResults, FileSystem fileSystem, Path tablePath) throws Exception{
        String outputFilePathString = tablePath.toString() + OUTPUT_FILE_NAME;
        Path outputFilePath = new Path(outputFilePathString);

        ClassLoader prevClassLoader = ClassLoaderStack.addJarFile(
                new Path(new Path(new SqoopOptions().getJarOutputDir()), testCase.getTableName() + ".jar").toString(),
                testCase.getTableName());

        try (SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, outputFilePath, new Configuration())) {
            WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
            Writable value = (Writable) reader.getValueClass().newInstance();
            boolean hasNextRecord = reader.next(key, value);
            int i = 0;

            if (!hasNextRecord && expectedResults != null && expectedResults.length > 0) {
                fail("Empty output file was not expected");
            }

            while (hasNextRecord) {
                assertEquals(expectedResults[i++], value.toString());
                hasNextRecord = reader.next(key, value);
            }
        } catch (IOException ioe) {
            LOG.error("Issue with verifying the output", ioe);
            throw new RuntimeException(ioe);
        }

        ClassLoaderStack.setCurrentClassLoader(prevClassLoader);

    }
}
