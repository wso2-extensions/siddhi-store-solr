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

package org.wso2.siddhi.extensions.store.solr.test;

import org.junit.AfterClass;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.extensions.store.solr.exceptions.SolrClientServiceException;
import org.wso2.siddhi.extensions.store.solr.impl.SolrClientServiceImpl;

/**
 * This test class contains the test cases related to inserting the events to solr event table
 */
public class InsertToSolrTableTestCase {
    @Test
    public void insertEventsToSolrEventTable() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String defineQuery =
                "define stream FooStream (time long, date string);" +
                "@store(type='solr', url='localhost:9983', collection='TEST2', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema='time long stored, date string stored', commit.async='true') " +
                "define table FooTable(time long, date string);";
        String insertQuery = "" +
                             "@info(name = 'query1') " +
                             "from FooStream   " +
                             "insert into FooTable ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(defineQuery + insertQuery);
        InputHandler fooTable = executionPlanRuntime.getInputHandler("FooStream");
        try {
            executionPlanRuntime.start();
            fooTable.send(new Object[]{45324211L, "1970-03-01 23:34:34 456"});
            fooTable.send(new Object[]{Long.MIN_VALUE, "2016-03-01 23:34:34 456"});
            fooTable.send(new Object[]{Long.MAX_VALUE, "2005-03-01 23:34:34 456"});
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void insertEventsToSolrEventTableWithPrimaryKeys() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String defineQuery =
                "define stream FooStream (firstname string, lastname string, age int);" +
                "@PrimaryKey('firstname','lastname')" +
                "@store(type='solr', url='localhost:9983', collection='TEST3', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema='firstname string stored, lastname string stored, age int stored', " +
                "commit.async='true')" +
                "define table FooTable(firstname string, lastname string, age int);";
        String insertQuery = "" +
                             "@info(name = 'query1') " +
                             "from FooStream   " +
                             "insert into FooTable ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(defineQuery + insertQuery);
        InputHandler fooStream = executionPlanRuntime.getInputHandler("FooStream");
        try {
            executionPlanRuntime.start();
            fooStream.send(new Object[]{"first1", "last1", 23});
            fooStream.send(new Object[]{"first2", "last2", 45});
            fooStream.send(new Object[]{"first1", "last1", 100});
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @AfterClass
    public static void deleteTables() throws Exception {
        SolrClientServiceImpl indexerService = SolrClientServiceImpl.getInstance();
        if (indexerService == null) {
            throw new SolrClientServiceException("Indexer Service cannot be loaded!");
        }
        try {
            indexerService.deleteCollection("TEST2");
            indexerService.deleteCollection("TEST3");
        } finally {
            indexerService.destroy();
        }
    }
}
