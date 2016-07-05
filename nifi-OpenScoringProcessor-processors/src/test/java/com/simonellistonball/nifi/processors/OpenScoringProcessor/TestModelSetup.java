package com.simonellistonball.nifi.processors.OpenScoringProcessor;

import org.apache.commons.httpclient.HttpClient;
import org.junit.Test;
import org.mockito.Mockito;

public class TestModelSetup {

    @Test
    public void testThatSettingModelPostsToOpenscoring() {
        HttpClient mockHttpClient = Mockito.mock(HttpClient.class);
        String responseBody = "";

    }

}
