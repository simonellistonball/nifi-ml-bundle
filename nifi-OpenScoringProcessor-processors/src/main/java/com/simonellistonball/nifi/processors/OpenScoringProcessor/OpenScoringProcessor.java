/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.simonellistonball.nifi.processors.OpenScoringProcessor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@Tags({ "ml" })
@SideEffectFree
@SupportsBatching
@CapabilityDescription("Scores a PMML model via Open Scoring")
@WritesAttributes({ @WritesAttribute(attribute = "prediction", description = "The prediction based on the model") })
public class OpenScoringProcessor extends AbstractProcessor {

    public static final PropertyDescriptor PMML = new PropertyDescriptor.Builder().name("PMML").description("The XML of the PMML model used to score data flow.").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor OPENSCORING_URL = new PropertyDescriptor.Builder().name("OpenScoring URL").description("Base URL for the Open Scoring server").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).addValidator(StandardValidators.URL_VALIDATOR).build();

    public static final Relationship SUCCESS = new Relationship.Builder().name("Success").description("Success relationship").build();

    public static final Relationship FAILURE = new Relationship.Builder().name("Failure").description("Failure relationship").build();
    public static final Relationship FAILURE_MODEL = new Relationship.Builder().name("Model Failure").description("Failed to post model").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private final HttpClient httpClient = new HttpClient();

    private AtomicReference<String> id = new AtomicReference<String>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PMML);
        descriptors.add(OPENSCORING_URL);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        relationships.add(FAILURE_MODEL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (OPENSCORING_URL.equals(descriptor) || PMML.equals(descriptor)) {
            this.id.set(null);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        if (id.get() == null) {
            try {
                this.id.set(postModel(context.getProperty(OPENSCORING_URL).getValue(), context.getProperty(PMML).getValue()));
            } catch (IOException e) {
                getLogger().error("Failure to post model", e);
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, FAILURE_MODEL);
            }
        }

        final String openScoringUrl = context.getProperty(OPENSCORING_URL).getValue();

        try {
            final boolean isCsv = flowFile.getAttribute("mime.type") == "text/csv";

            StringBuilder urlBuilder = new StringBuilder(openScoringUrl).append("/model/").append(id);
            if (isCsv) {
                urlBuilder.append("/csv");
            }
            final PostMethod post = new PostMethod(urlBuilder.toString());

            final String contentType;
            if (isCsv) {
                contentType = "text/plain";
            } else {
                contentType = "application/json";
            }
            post.setRequestHeader("Content-Type", contentType);

            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    post.setRequestEntity(new InputStreamRequestEntity(in, contentType));
                }
            });

            httpClient.executeMethod(post);
            if (isCsv) {
                // add the results to the input
                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream out) throws IOException {
                        IOUtils.copy(post.getResponseBodyAsStream(), out);
                    }
                });
                session.transfer(flowFile, SUCCESS);
            } else {
                JsonParser parser = new JsonParser();
                JsonElement parsed = parser.parse(new BufferedReader(new InputStreamReader(post.getResponseBodyAsStream(), "UTF-8")));

                JsonObject newAttributes = parsed.getAsJsonObject().getAsJsonObject("result");
                final Map<String, String> attributes = new HashMap<String, String>(newAttributes.size());

                for (Entry<String, JsonElement> attribute : newAttributes.entrySet()) {
                    if (!attribute.getValue().isJsonNull()) {
                        attributes.put(attribute.getKey(), attribute.getValue().getAsString());
                    }
                }

                if (!attributes.isEmpty()) {
                    flowFile = session.putAllAttributes(flowFile, attributes);
                }
                session.transfer(flowFile, SUCCESS);
            }
        } catch (Exception e) {
            getLogger().error("Failure to score model", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, FAILURE);
        }

    }

    private String postModel(String openScoringUrl, String pmml) throws HttpException, IOException {
        id.set(UUID.randomUUID().toString());
        StringBuilder urlBuilder = new StringBuilder(openScoringUrl).append("/model/").append(id);
        String url = urlBuilder.toString();
        PutMethod put = new PutMethod(url);
        getLogger().warn(String.format("Posting model to %s", url));
        put.setRequestHeader("Content-Type", "text/xml");
        RequestEntity requestEntity = new StringRequestEntity(pmml, "text/xml", "utf-8");
        put.setRequestEntity(requestEntity);
        httpClient.executeMethod(put);
        return id.get();
    }
}
