/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.s3;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.sqoop.testutil.*;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static org.junit.Assume.assumeNotNull;

public class TestS3AvroImport extends ImportJobTestCase {

    public static final Log LOG = LogFactory.getLog(
            TestS3AvroImport.class.getName());

    private static S3CredentialGenerator s3CredentialGenerator;

    private FileSystem s3Client;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupS3Credentials() throws IOException {
        String generatorCommand = S3TestUtils.getGeneratorCommand();
        s3CredentialGenerator = new DefaultS3CredentialGenerator(generatorCommand);
    }

    @Before
    public void setup() throws IOException {
        assumeNotNull(s3CredentialGenerator.getS3AccessKey());
        assumeNotNull(s3CredentialGenerator.getS3SecretKey());

        super.setUp();

        S3TestUtils.createTestTableFromInputData(this);

        s3Client = S3TestUtils.setS3CredentialsInHadoopConfAndGetS3Client(s3CredentialGenerator);

        S3TestUtils.clearOutputDir(s3Client);
    }

    @After
    public void clearOutputDir() throws IOException {
        S3TestUtils.clearOutputDir(s3Client);
        super.tearDown();
    }

    protected ArgumentArrayBuilder getArgumentArrayBuilder() {
        ArgumentArrayBuilder builder = S3TestUtils.getArgumentArrayBuilderForUnitTests(this, s3CredentialGenerator);
        return builder;
    }


    @Test
    public void testImportAsAvroDataFileWithoutDeleteTargetDirOptionWhenTargetDirDoesntExist() throws IOException {
        ArgumentArrayBuilder builder = getArgumentArrayBuilder();
        builder.withOption("as-avrodatafile");
        String[] args = builder.build();
        runImport(args);
        AvroTestUtils.verify(S3TestUtils.getExpectedAvroOutput(), s3Client.getConf(), S3TestUtils.getTargetDirPath());
    }

    @Test
    public void testImportAsAvroDataFileWithDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws IOException {
        ArgumentArrayBuilder builder = getArgumentArrayBuilder();
        builder.withOption("as-avrodatafile");
        builder.withOption("delete-target-dir");
        String[] args = builder.build();
        runImport(args);
        AvroTestUtils.verify(S3TestUtils.getExpectedAvroOutput(), s3Client.getConf(), S3TestUtils.getTargetDirPath());

        runImport(args);
    }

    @Test
    public void testImportAsAvroDataFileWithoutDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws IOException {
        ArgumentArrayBuilder builder = getArgumentArrayBuilder();
        builder.withOption("as-avrodatafile");
        String[] args = builder.build();
        runImport(args);
        AvroTestUtils.verify(S3TestUtils.getExpectedAvroOutput(), s3Client.getConf(), S3TestUtils.getTargetDirPath());

        thrown.expect(IOException.class);
        runImport(args);
    }

}
