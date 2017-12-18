/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.store.solr.test;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.wso2.extension.siddhi.store.solr.exceptions.SolrClientServiceException;
import org.wso2.extension.siddhi.store.solr.impl.SiddhiSolrClient;
import org.wso2.extension.siddhi.store.solr.impl.SolrClientServiceImpl;

import java.io.IOException;

class SolrTestUtils {

    static void waitTillEventsPersist(SolrClientServiceImpl service, int count, String table,
                                      Duration duration) {
        waitTillEventsPersist(service, "*:*", count, table, duration);
    }

    static void waitTillEventsPersist(SolrClientServiceImpl service, String strQuery, int count, String table,
                                      Duration duration) {
        waitTillSolrCollection(service, table, Duration.TEN_SECONDS);
        Awaitility.await().atMost(duration).until(() -> {
            SolrQuery query = new SolrQuery(strQuery);
            query.setStart(0);
            query.setRows(100);
            SolrClient client = service.getSolrServiceClientByCollection(table);
            QueryResponse response = client.query(table, query);
            return response.getResults().getNumFound() == count;
        });
    }

    static void waitTillVariableCountMatches(long actual, long expected, Duration duration) {
        Awaitility.await().atMost(duration).until(() -> actual == expected);
    }

    static void waitTillSolrCollection(SolrClientServiceImpl service, String collection, Duration duration) {
        Awaitility.await().atMost(duration).until(() -> service.collectionExists(collection));
    }

    static long getDocCount(SolrClientServiceImpl indexerService, String collection)
            throws SolrClientServiceException, IOException, SolrServerException {
        SiddhiSolrClient client;
        client = indexerService.getSolrServiceClientByCollection(collection);
        SolrQuery solrQuery = new SolrQuery("*:*");
        solrQuery.setRows(0);
        return client.query(collection, solrQuery).getResults().getNumFound();
    }
}
