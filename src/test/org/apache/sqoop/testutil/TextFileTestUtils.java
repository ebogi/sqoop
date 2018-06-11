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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TextFileTestUtils {

    private static final String OUTPUT_FILE_NAME = "/part-m-00000";

    public static final Log LOG = LogFactory.getLog(
            TextFileTestUtils.class.getName());

    /**
     * Verify results at the given tablePath.
     * @param expectedResults string array of expected results
     * @param fileSystem current filesystem
     * @param tablePath path of the output table
     */
    public static void verify(String[] expectedResults, FileSystem fileSystem, Path tablePath) throws IOException {
        String outputFilePathString = tablePath.toString() + OUTPUT_FILE_NAME;
        Path outputFilePath = new Path(outputFilePathString);

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(outputFilePath), Charset.forName("UTF-8")))) {
            String line = br.readLine();
            int i = 0;

            if (line == null && expectedResults != null && expectedResults.length > 0) {
                fail("Empty output file was not expected");
            }

            while (line != null) {
                assertEquals(expectedResults[i++], line);
                line = br.readLine();
            }

        } catch (IOException ioe) {
            LOG.error("Issue with verifying the output", ioe);
            throw new RuntimeException(ioe);
        }
    }
}
