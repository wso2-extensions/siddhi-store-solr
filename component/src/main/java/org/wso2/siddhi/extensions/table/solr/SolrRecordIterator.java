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

package org.wso2.siddhi.extensions.table.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.wso2.siddhi.core.table.record.RecordIterator;
import org.wso2.siddhi.extensions.table.solr.exceptions.SolrClientServiceException;
import org.wso2.siddhi.extensions.table.solr.exceptions.SolrIteratorException;
import org.wso2.siddhi.extensions.table.solr.impl.SiddhiSolrClient;
import org.wso2.siddhi.extensions.table.solr.impl.SolrClientServiceImpl;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class represents the iterator which streams a set solr documents
 */
public class SolrRecordIterator implements RecordIterator<Object[]> {

    private SiddhiSolrClient solrClient;
    private int batchSize;
    private List<Attribute> attributes;
    private SolrDocumentList solrDocuments;
    private Iterator<SolrDocument> solrDocumentIterator;
    private SolrQuery query;
    private String solrCollection;
    private int start;
    private int count;

    public SolrRecordIterator(String condition, SolrClientServiceImpl service, String collection, int batchSize,
                              List<Attribute> attributes)
            throws SolrClientServiceException {
        this.batchSize = batchSize;
        this.attributes = attributes;
        this.solrClient = service.getSolrServiceClient();
        this.solrCollection = collection;
        this.query = new SolrQuery(condition);
        this.start = 0;
        this.count = batchSize;
    }

    @Override
    public void close() throws IOException {
        this.solrClient = null;
    }

    @Override
    public boolean hasNext() {
        synchronized (this) {
            try {
                if (solrDocumentIterator != null && solrDocuments != null) {
                    if (solrDocumentIterator.hasNext()) {
                        return true;
                    } else {
                        if (solrDocuments.size() < batchSize) {
                            return false;
                        } else {
                            query.setStart(start);
                            query.setRows(count);
                            start += count;
                            solrDocuments = solrClient.query(solrCollection, query).getResults();
                            solrDocumentIterator = solrDocuments.iterator();
                            return hasNext();
                        }
                    }
                } else {
                    query.setStart(start);
                    query.setRows(count);
                    start += count;
                    solrDocuments = solrClient.query(solrCollection, query).getResults();
                    solrDocumentIterator = solrDocuments.iterator();
                    return hasNext();
                }
            } catch (SolrServerException | IOException e) {
                throw new SolrIteratorException("Error while calling hasNext(): " + e.getMessage(), e);
            }
        }
    }

    @Override
    public Object[] next() {
        synchronized (this) {
            List<Object> fieldValues = new ArrayList<>();
            if (hasNext() && solrDocumentIterator != null) {
                SolrDocument solrDocument = solrDocumentIterator.next();
                for (Attribute attribute : attributes) {
                    Object fieldValue = solrDocument.getFieldValue(attribute.getName());
                    fieldValues.add(fieldValue);
                }
            }
            return fieldValues.toArray();
        }
    }

    public SolrDocument nextDocument() {
        synchronized (this) {
            if (hasNext() && solrDocumentIterator != null) {
                return solrDocumentIterator.next();
            } else {
                return null;
            }
        }
    }
}
