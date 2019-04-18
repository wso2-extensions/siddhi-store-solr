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

package org.wso2.extension.siddhi.store.solr.test;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.store.solr.beans.SolrSchema;
import org.wso2.extension.siddhi.store.solr.beans.SolrSchemaField;
import org.wso2.extension.siddhi.store.solr.exceptions.SolrClientServiceException;
import org.wso2.extension.siddhi.store.solr.exceptions.SolrSchemaNotFoundException;
import org.wso2.extension.siddhi.store.solr.impl.SolrClientServiceImpl;

/**
 * This class contains the test cases related to SolrEventTable
 */

public class DefineSolrTableTestCase {

    private static SolrClientServiceImpl indexerService;

    @Test
    public void testDefineSolrTable() throws SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String defineQuery =
                "@store(type='solr', collection='TEST1', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema ='time long stored, date string stored', commit.async='true') " +
                "define table Footable(time long, date string);";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(defineQuery);
        siddhiAppRuntime.start();
        indexerService = SolrClientServiceImpl.INSTANCE;
        try {
            Assert.assertTrue(indexerService.collectionExists("TEST1"));
            SolrSchema schema = indexerService.getSolrSchema("TEST1");
            SolrSchemaField field1 = schema.getField("time");
            SolrSchemaField field2 = schema.getField("date");
            Assert.assertTrue(field1.getProperty(SolrSchemaField.ATTR_STORED).equals(true));
            Assert.assertTrue(field1.getProperty(SolrSchemaField.ATTR_TYPE).equals("long"));
            Assert.assertTrue(field2.getProperty(SolrSchemaField.ATTR_STORED).equals(true));
            Assert.assertTrue(field2.getProperty(SolrSchemaField.ATTR_TYPE).equals("string"));
        } catch (SolrClientServiceException | SolrSchemaNotFoundException e) {
            Assert.fail(e.getMessage());
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test(dependsOnMethods = "testDefineSolrTable")
    public void testDefineExistingSolrTable() throws SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String defineQuery =
                "@store(type='solr', collection='TEST1', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema ='time long stored, date string stored', commit.async='true') " +
                "define table Footable(time long, date string);";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(defineQuery);
        try {
            siddhiAppRuntime.start();
            indexerService = SolrClientServiceImpl.INSTANCE;
        } finally {
            indexerService.deleteCollection("TEST1");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testDefineSolrTable2() throws SolrClientServiceException, SolrSchemaNotFoundException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String defineQuery =
                "@store(type='solr', zookeeper.url='localhost:3456', collection='TEST2', base" +
                ".config='gettingstarted', " +
                "shards='2', replicas='2', schema ='time long stored, date string stored', commit.async='false') " +
                "define table Footable(time long, date string);";

        SiddhiAppRuntime runtime = siddhiManager.createSiddhiAppRuntime(defineQuery);
        runtime.start();
        try {
            Awaitility.await().atLeast(Duration.FIVE_SECONDS);
        } finally {
            runtime.shutdown();
        }
    }

    @Test
    public void testDefineSolrTable3() throws SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String defineQuery =
                "@store(type='solr', collection='XXX', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema ='time long stored, date string stored') " +
                "define table Footable(time long, date string);";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(defineQuery);
        siddhiAppRuntime.start();
        indexerService = SolrClientServiceImpl.INSTANCE;
        try {
            Assert.assertTrue(indexerService.collectionExists("XXX"));
            SolrSchema schema = indexerService.getSolrSchema("XXX");
            SolrSchemaField field1 = schema.getField("time");
            SolrSchemaField field2 = schema.getField("date");
            Assert.assertTrue(field1.getProperty(SolrSchemaField.ATTR_STORED).equals(true));
            Assert.assertTrue(field1.getProperty(SolrSchemaField.ATTR_TYPE).equals("long"));
            Assert.assertTrue(field2.getProperty(SolrSchemaField.ATTR_STORED).equals(true));
            Assert.assertTrue(field2.getProperty(SolrSchemaField.ATTR_TYPE).equals("string"));
        } catch (SolrClientServiceException | SolrSchemaNotFoundException e) {
            Assert.fail(e.getMessage());
        } finally {
            indexerService.deleteCollection("XXX");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testDefineSolrTable4() throws SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String defineQuery =
                "@store(type='solr', collection='YYY', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema ='time long stored, date string stored', commit.async='') " +
                "define table Footable(time long, date string);";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(defineQuery);
        siddhiAppRuntime.start();
        indexerService = SolrClientServiceImpl.INSTANCE;
        try {
            Assert.assertTrue(indexerService.collectionExists("YYY"));
            SolrSchema schema = indexerService.getSolrSchema("YYY");
            SolrSchemaField field1 = schema.getField("time");
            SolrSchemaField field2 = schema.getField("date");
            Assert.assertTrue(field1.getProperty(SolrSchemaField.ATTR_STORED).equals(true));
            Assert.assertTrue(field1.getProperty(SolrSchemaField.ATTR_TYPE).equals("long"));
            Assert.assertTrue(field2.getProperty(SolrSchemaField.ATTR_STORED).equals(true));
            Assert.assertTrue(field2.getProperty(SolrSchemaField.ATTR_TYPE).equals("string"));
        } catch (SolrClientServiceException | SolrSchemaNotFoundException e) {
            Assert.fail(e.getMessage());
        } finally {
            indexerService.deleteCollection("YYY");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testDefineSolrTable5() throws SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String defineQuery =
                "@store(type='solr', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema ='time long stored, date string stored', commit.async='false') " +
                "define table Footable(time long, date string);";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(defineQuery);
        siddhiAppRuntime.start();
        indexerService = SolrClientServiceImpl.INSTANCE;
        try {
            Assert.assertTrue(indexerService.collectionExists("Footable"));
            SolrSchema schema = indexerService.getSolrSchema("Footable");
            SolrSchemaField field1 = schema.getField("time");
            SolrSchemaField field2 = schema.getField("date");
            Assert.assertTrue(field1.getProperty(SolrSchemaField.ATTR_STORED).equals(true));
            Assert.assertTrue(field1.getProperty(SolrSchemaField.ATTR_TYPE).equals("long"));
            Assert.assertTrue(field2.getProperty(SolrSchemaField.ATTR_STORED).equals(true));
            Assert.assertTrue(field2.getProperty(SolrSchemaField.ATTR_TYPE).equals("string"));
        } catch (SolrClientServiceException | SolrSchemaNotFoundException e) {
            Assert.fail(e.getMessage());
        } finally {
            indexerService.deleteCollection("Footable");
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testDefineSolrTable6() throws SolrClientServiceException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String defineQuery =
                "@store(type='solr', collection='', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema ='time long stored, date string stored', commit.async='false') " +
                "define table Footable2(time long, date string);";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(defineQuery);
        siddhiAppRuntime.start();
        indexerService = SolrClientServiceImpl.INSTANCE;
        try {
            Assert.assertTrue(indexerService.collectionExists("Footable2"));
            SolrSchema schema = indexerService.getSolrSchema("Footable2");
            SolrSchemaField field1 = schema.getField("time");
            SolrSchemaField field2 = schema.getField("date");
            Assert.assertTrue(field1.getProperty(SolrSchemaField.ATTR_STORED).equals(true));
            Assert.assertTrue(field1.getProperty(SolrSchemaField.ATTR_TYPE).equals("long"));
            Assert.assertTrue(field2.getProperty(SolrSchemaField.ATTR_STORED).equals(true));
            Assert.assertTrue(field2.getProperty(SolrSchemaField.ATTR_TYPE).equals("string"));
        } catch (SolrClientServiceException | SolrSchemaNotFoundException e) {
            Assert.fail(e.getMessage());
        } finally {
            indexerService.deleteCollection("Footable2");
            siddhiAppRuntime.shutdown();
        }
    }
}
