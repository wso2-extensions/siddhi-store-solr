/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extensions.siddhi.store.solr.test;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.extensions.siddhi.store.solr.beans.SiddhiSolrDocument;
import org.wso2.extensions.siddhi.store.solr.beans.SolrSchema;
import org.wso2.extensions.siddhi.store.solr.beans.SolrSchemaField;
import org.wso2.extensions.siddhi.store.solr.config.CollectionConfiguration;
import org.wso2.extensions.siddhi.store.solr.exceptions.SolrClientServiceException;
import org.wso2.extensions.siddhi.store.solr.exceptions.SolrSchemaNotFoundException;
import org.wso2.extensions.siddhi.store.solr.impl.SiddhiSolrClient;
import org.wso2.extensions.siddhi.store.solr.impl.SolrClientServiceImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class contains the unit tests for indexer service;
 */
public class CarbonIndexerServiceTestCase {

    private static final String TABLE_T1 = "T1";
    private static SolrClientServiceImpl indexerService;


    @BeforeClass
    public static void init() throws SolrClientServiceException {
        indexerService = SolrClientServiceImpl.INSTANCE;
    }

    @Test
    public void step1_testCreateIndexForTable() throws SolrClientServiceException {
        CollectionConfiguration config = new CollectionConfiguration.Builder().collectionName(TABLE_T1).shards(2)
                .replicas(2).configs("gettingstarted").solrServerUrl("localhost:9983").build();
        indexerService.initCollection(config);
        Assert.assertTrue(indexerService.collectionExists(TABLE_T1));
    }

    @Test(dependsOnMethods = "step1_testCreateIndexForTable")
    public void step2_testCreateExistingIndex() throws SolrClientServiceException {
        CollectionConfiguration config = new CollectionConfiguration.Builder().collectionName(TABLE_T1).shards(2)
                .replicas(2).configs("gettingstarted").solrServerUrl("localhost:9983").build();
        Assert.assertTrue(indexerService.collectionExists(TABLE_T1));
        Assert.assertFalse(indexerService.initCollection(config));
    }

    @Test(dependsOnMethods = "step2_testCreateExistingIndex")
    public void step3_testIfInitialConfigsAreCopied() throws SolrClientServiceException {
        Assert.assertTrue(indexerService.collectionConfigExists(TABLE_T1));
    }

    @Test(dependsOnMethods = "step3_testIfInitialConfigsAreCopied")
    public void step4_testNonExistingIndexSchema() throws SolrClientServiceException {
        try {
            indexerService.getSolrSchema("T2");
        } catch (Exception e) {
            Assert.assertTrue((e instanceof SolrSchemaNotFoundException) || e instanceof SolrClientServiceException);
        }
    }

    @Test(dependsOnMethods = "step4_testNonExistingIndexSchema")
    public void step5_testInitialIndexSchema() throws SolrClientServiceException, SolrSchemaNotFoundException {
        SolrSchema indexSchema = indexerService.getSolrSchema(TABLE_T1);
        Assert.assertEquals("id", indexSchema.getUniqueKey());
    }

    @Test(dependsOnMethods = "step5_testInitialIndexSchema")
    public void step6_testUpdateIndexSchemaWithoutMerge()
            throws SolrSchemaNotFoundException, SolrClientServiceException {
        Map<String, SolrSchemaField> fieldMap = new HashMap<>();
        SolrSchemaField intField = new SolrSchemaField();
        intField.setProperty("name", "IntField");
        intField.setProperty("indexed", true);
        intField.setProperty("type", "int");
        intField.setProperty("stored", true);
        SolrSchemaField longField = new SolrSchemaField();
        longField.setProperty("name", "LongField");
        longField.setProperty("indexed", true);
        longField.setProperty("type", "long");
        longField.setProperty("stored", true);
        SolrSchemaField floatField = new SolrSchemaField();
        floatField.setProperty("name", "FloatField");
        floatField.setProperty("indexed", true);
        floatField.setProperty("type", "float");
        floatField.setProperty("stored", true);
        SolrSchemaField doubleField = new SolrSchemaField();
        doubleField.setProperty("name", "DoubleField");
        doubleField.setProperty("indexed", true);
        doubleField.setProperty("type", "double");
        floatField.setProperty("stored", true);
        SolrSchemaField boolField = new SolrSchemaField();
        boolField.setProperty("name", "BoolField");
        boolField.setProperty("indexed", true);
        boolField.setProperty("type", "boolean");
        boolField.setProperty("stored", true);
        SolrSchemaField timestamp = new SolrSchemaField();
        timestamp.setProperty("name", "_timestamp");
        timestamp.setProperty("indexed", true);
        timestamp.setProperty("type", "long");
        timestamp.setProperty("stored", true);
        fieldMap.put(intField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), intField);
        fieldMap.put(longField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), longField);
        fieldMap.put(doubleField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), doubleField);
        fieldMap.put(floatField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), floatField);
        fieldMap.put(boolField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), boolField);
        fieldMap.put(timestamp.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), timestamp);
        SolrSchema indexSchema = new SolrSchema("id", fieldMap);
        Assert.assertTrue(indexerService.updateSolrSchema(TABLE_T1, indexSchema, false));
        SolrSchema indexSchema1 = indexerService.getSolrSchema(TABLE_T1);
        Assert.assertEquals(indexSchema.getField("IntField"), indexSchema1.getField("IntField"));
        Assert.assertEquals(indexSchema.getField("LongField"), indexSchema1.getField("LongField"));
        Assert.assertEquals(indexSchema.getField("DoubleField"), indexSchema1.getField("DoubleField"));
        Assert.assertEquals(indexSchema.getField("FloatField"), indexSchema1.getField("FloatField"));
        Assert.assertEquals(indexSchema.getField("BoolField"), indexSchema1.getField("BoolField"));
        Assert.assertEquals(indexSchema.getField("_timestamp"), indexSchema1.getField("_timestamp"));
    }

    @Test(dependsOnMethods = "step6_testUpdateIndexSchemaWithoutMerge")
    public void step7_testUpdateIndexSchemaWithMerge()
            throws SolrSchemaNotFoundException, SolrClientServiceException {
        SolrSchema oldIndexSchema = indexerService.getSolrSchema(TABLE_T1);
        Map<String, SolrSchemaField> fieldMap = new HashMap<>();
        SolrSchemaField intField = new SolrSchemaField();
        intField.setProperty("name", "IntField1");
        intField.setProperty("indexed", true);
        intField.setProperty("type", "int");
        intField.setProperty("stored", true);
        SolrSchemaField longField = new SolrSchemaField();
        longField.setProperty("name", "LongField1");
        longField.setProperty("indexed", true);
        longField.setProperty("type", "long");
        longField.setProperty("stored", true);
        SolrSchemaField floatField = new SolrSchemaField();
        floatField.setProperty("name", "FloatField1");
        floatField.setProperty("indexed", true);
        floatField.setProperty("type", "float");
        floatField.setProperty("stored", true);
        SolrSchemaField doubleField = new SolrSchemaField();
        doubleField.setProperty("name", "DoubleField1");
        doubleField.setProperty("indexed", true);
        doubleField.setProperty("type", "double");
        doubleField.setProperty("stored", true);
        SolrSchemaField boolField = new SolrSchemaField();
        boolField.setProperty("name", "BoolField1");
        boolField.setProperty("indexed", true);
        boolField.setProperty("type", "boolean");
        boolField.setProperty("stored", true);
        SolrSchemaField stringField = new SolrSchemaField();
        stringField.setProperty("name", "StringField1");
        stringField.setProperty("indexed", true);
        stringField.setProperty("type", "string");
        stringField.setProperty("stored", true);
        fieldMap.put(intField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), intField);
        fieldMap.put(longField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), longField);
        fieldMap.put(doubleField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), doubleField);
        fieldMap.put(floatField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), floatField);
        fieldMap.put(boolField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), boolField);
        fieldMap.put(stringField.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), stringField);
        SolrSchema indexSchema = new SolrSchema("id", fieldMap);
        Assert.assertTrue(indexerService.updateSolrSchema(TABLE_T1, indexSchema, true));
        SolrSchema newIndexSchema = indexerService.getSolrSchema(TABLE_T1);
        Assert.assertEquals(indexSchema.getField("IntField1"), newIndexSchema.getField("IntField1"));
        Assert.assertEquals(indexSchema.getField("LongField1"), newIndexSchema.getField("LongField1"));
        Assert.assertEquals(indexSchema.getField("DoubleField1"), newIndexSchema.getField("DoubleField1"));
        Assert.assertEquals(indexSchema.getField("FloatField1"), newIndexSchema.getField("FloatField1"));
        Assert.assertEquals(indexSchema.getField("BoolField1"), newIndexSchema.getField("BoolField1"));
        Assert.assertEquals(indexSchema.getField("StringField1"), newIndexSchema.getField("StringField1"));

        Assert.assertEquals(oldIndexSchema.getField("IntField"), newIndexSchema.getField("IntField"));
        Assert.assertEquals(oldIndexSchema.getField("LongField"), newIndexSchema.getField("LongField"));
        Assert.assertEquals(oldIndexSchema.getField("DoubleField"), newIndexSchema.getField("DoubleField"));
        Assert.assertEquals(oldIndexSchema.getField("FloatField"), newIndexSchema.getField("FloatField"));
        Assert.assertEquals(oldIndexSchema.getField("BoolField"), newIndexSchema.getField("BoolField"));
        Assert.assertEquals(oldIndexSchema.getField("_timestamp"), newIndexSchema.getField("_timestamp"));
    }

    @Test(dependsOnMethods = "step7_testUpdateIndexSchemaWithMerge")
    public void step8_testIndexDocuments() throws SolrClientServiceException, IOException, SolrServerException {
        SiddhiSolrDocument doc1 = new SiddhiSolrDocument();
        doc1.addField("id", "1");
        doc1.addField("_timestamp", System.currentTimeMillis());
        doc1.addField("IntField", 100);
        doc1.addField("LongField", 100L);
        doc1.addField("FloatField", 100f);
        doc1.addField("DoubleField", 100d);
        doc1.addField("BoolField", true);
        SiddhiSolrDocument doc2 = new SiddhiSolrDocument();
        doc2.addField("id", "2");
        doc2.addField("IntField1", 1000);
        doc2.addField("LongField1", 1000L);
        doc2.addField("FloatField1", 1000f);
        doc2.addField("DoubleField1", 1000d);
        doc2.addField("BoolField1", true);
        doc2.addField("StringField1", "The quick brown fox jumps over the lazy dog");
        List<SiddhiSolrDocument> docs = new ArrayList<>();
        docs.add(doc1);
        docs.add(doc2);
        indexerService.insertDocuments(TABLE_T1, docs, false);

        SiddhiSolrClient client = indexerService.getSolrServiceClientByCollection(TABLE_T1);
        SolrQuery query = new SolrQuery();
        query.setQuery("id:1");

        QueryResponse response = client.query(TABLE_T1, query);
        SolrDocumentList list = response.getResults();
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(doc1.getFieldValue("id"), list.get(0).getFieldValue("id"));
        Assert.assertEquals(doc1.getFieldValue("DoubleField"), list.get(0).getFieldValue("DoubleField"));

        query = new SolrQuery();
        query.setQuery("id:2");
        QueryResponse response1 = client.query(TABLE_T1, query);
        SolrDocumentList list1 = response1.getResults();
        Assert.assertEquals(1, list1.size());
        Assert.assertEquals(doc2.getFieldValue("id"), list1.get(0).getFieldValue("id"));
    }

    @Test(dependsOnMethods = "step8_testIndexDocuments")
    public void step9_testDeleteDocumentsByTimeRange()
            throws SolrClientServiceException, IOException, SolrServerException {
        String strQuery = "_timestamp:[0 TO " + System.currentTimeMillis() + "]";
        indexerService.deleteDocuments(TABLE_T1, strQuery, false);
        SiddhiSolrClient client = indexerService.getSolrServiceClientByCollection(TABLE_T1);
        SolrQuery query = new SolrQuery();
        query.setQuery(strQuery);
        QueryResponse response = client.query(TABLE_T1, query);
        SolrDocumentList list = response.getResults();
        Assert.assertTrue(list.isEmpty());
    }

    @Test(dependsOnMethods = "step9_testDeleteDocumentsByTimeRange")
    public void stepA_testDeleteDocumentsByIds() throws SolrClientServiceException, IOException,
                                                  SolrServerException {
        List<String> ids = new ArrayList<>();
        ids.add("2");
        indexerService.deleteDocuments(TABLE_T1, ids, false);
        SiddhiSolrClient client = indexerService.getSolrServiceClientByCollection(TABLE_T1);
        SolrQuery query = new SolrQuery();
        query.setQuery("id:2");
        QueryResponse response = client.query(TABLE_T1, query);
        SolrDocumentList list = response.getResults();
        Assert.assertTrue(list.isEmpty());
    }

    @Test(dependsOnMethods = "stepA_testDeleteDocumentsByIds")
    public void stepB_testDeleteIndexForTable() throws SolrClientServiceException {
        Assert.assertTrue(indexerService.deleteCollection(TABLE_T1));
    }

    @Test(dependsOnMethods = "stepB_testDeleteIndexForTable")
    public void stepC_testDeleteNonExistingIndex() throws SolrClientServiceException {
        Assert.assertFalse(indexerService.deleteCollection(TABLE_T1));
    }

    @AfterClass
    public static void stepD_testDestroy() throws SolrClientServiceException {
        indexerService.destroy();
    }
}
