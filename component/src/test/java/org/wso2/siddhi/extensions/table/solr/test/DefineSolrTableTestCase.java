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

package org.wso2.siddhi.extensions.table.solr.test;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.extensions.table.solr.beans.SolrSchema;
import org.wso2.siddhi.extensions.table.solr.beans.SolrSchemaField;
import org.wso2.siddhi.extensions.table.solr.exceptions.SolrClientServiceException;
import org.wso2.siddhi.extensions.table.solr.exceptions.SolrSchemaNotFoundException;
import org.wso2.siddhi.extensions.table.solr.impl.SolrClientServiceImpl;

/**
 * This class contains the test cases related to SolrEventTable
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DefineSolrTableTestCase {

    private static SolrClientServiceImpl indexerService;

    @Test
    public void testDefineSolrTable() {
        SiddhiManager siddhiManager = new SiddhiManager();
        String defineQuery =
                "@store(type='solr', url='localhost:9983', collection='TEST1', base.config='gettingstarted', " +
                "shards='2', replicas='2', schema ='time long stored, date string stored', commit.async='true') " +
                "define table Footable(time long, date string);";

        siddhiManager.createExecutionPlanRuntime(defineQuery);
        indexerService = SolrClientServiceImpl.getInstance();
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
        }
    }

    @AfterClass
    public static void deleteTables() throws Exception {
        try {
            indexerService.deleteCollection("TEST1");
        } finally {
            if (indexerService != null) {
                indexerService.destroy();
            }
        }
    }
}
