package com.simonellistonball.nifi.processors.OpenScoringProcessor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestOpenScoringProcessor {

    private static final String JSON_SNIPPET = "/iris.json";
    private static final String IRIS_SNIPPET = "/iris.csv";
    private static final String PMML_FILE = "/pmml.xml";

    @Before
    public void setupOpenScoringServer() {

    }

    @Test
    public void testValidations() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(OpenScoringProcessor.class);

        Collection<ValidationResult> results;
        ProcessContext pc;
        results = new HashSet<>();
        runner.enqueue(new byte[0]);
        runner.setProperty(OpenScoringProcessor.OPENSCORING_URL, "http://localhost:8080/openscoring");
        runner.setProperty(OpenScoringProcessor.PMML, IOUtils.toString(getClass().getResourceAsStream(PMML_FILE)));

        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(0, results.size());
    }

    @SuppressWarnings("serial")
    @Test
    public void testPmmlScoreJsonData() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(OpenScoringProcessor.class);
        InputStream in = getClass().getResourceAsStream(JSON_SNIPPET);
        runner.enqueue(in, new HashMap<String, String>() {
            {
                put("mime.type", "text/json");
            }
        });
        testPmmlScore(runner);

        for (MockFlowFile f : runner.getFlowFilesForRelationship(OpenScoringProcessor.SUCCESS)) {
            f.assertAttributeExists("class");
            f.assertAttributeEquals("class", "Iris-setosa");
        }
    }

    @SuppressWarnings("serial")
    @Test
    public void testPmmlScoreCSVData() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(OpenScoringProcessor.class);
        runner.enqueue(getClass().getResourceAsStream(IRIS_SNIPPET), new HashMap<String, String>() {
            {
                put("mime.type", "text/csv");
            }
        });
        testPmmlScore(runner);
    }

    private void testPmmlScore(TestRunner runner) throws IOException {
        runner.setProperty(OpenScoringProcessor.OPENSCORING_URL, "http://localhost:8080/openscoring");
        runner.setProperty(OpenScoringProcessor.PMML, IOUtils.toString(getClass().getResourceAsStream(PMML_FILE)));
        runner.run();
        runner.assertAllFlowFilesTransferred(OpenScoringProcessor.SUCCESS);

    }

}
