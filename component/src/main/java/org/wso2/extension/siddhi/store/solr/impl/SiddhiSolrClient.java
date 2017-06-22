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

package org.wso2.extension.siddhi.store.solr.impl;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.wso2.extension.siddhi.store.solr.utils.SolrTableUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * This class represents the Custom SolrClient implementation used for indexing
 */
public class SiddhiSolrClient extends SolrClient {

    private static final long serialVersionUID = -6070746447077156187L;
    private SolrClient solrClient;
    private String domain;

    public SiddhiSolrClient(String domain, SolrClient solrClient) {
        this.solrClient = solrClient;
        this.domain = domain;
    }

    public UpdateResponse add(String collection, Collection<SolrInputDocument> docs)
            throws SolrServerException, IOException {
        return solrClient.add(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), docs);
    }

    public UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
        return solrClient.add(docs);
    }

    public UpdateResponse add(String collection, Collection<SolrInputDocument> docs, int commitWithinMs)
            throws SolrServerException, IOException {
        return solrClient.add(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), docs, commitWithinMs);
    }

    public UpdateResponse add(Collection<SolrInputDocument> docs, int commitWithinMs)
            throws SolrServerException, IOException {
        return solrClient.add(docs, commitWithinMs);
    }

    public UpdateResponse add(String collection, SolrInputDocument doc) throws SolrServerException, IOException {
        return solrClient.add(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), doc);
    }

    public UpdateResponse add(SolrInputDocument doc) throws SolrServerException, IOException {
        return solrClient.add(doc);
    }

    public UpdateResponse add(String collection, SolrInputDocument doc, int commitWithinMs)
            throws SolrServerException, IOException {
        return solrClient.add(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), doc, commitWithinMs);
    }

    public UpdateResponse add(SolrInputDocument doc, int commitWithinMs) throws SolrServerException, IOException {
        return solrClient.add(doc, commitWithinMs);
    }

    public UpdateResponse add(String collection, Iterator<SolrInputDocument> docIterator)
            throws SolrServerException, IOException {
        return solrClient.add(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), docIterator);
    }

    public UpdateResponse add(Iterator<SolrInputDocument> docIterator)
            throws SolrServerException, IOException {
        return solrClient.add(docIterator);
    }

    public UpdateResponse addBean(String collection, Object obj) throws IOException, SolrServerException {
        return solrClient.addBean(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), obj);
    }

    public UpdateResponse addBean(Object obj) throws IOException, SolrServerException {
        return solrClient.addBean(obj);
    }

    public UpdateResponse addBean(String collection, Object obj, int commitWithinMs)
            throws IOException, SolrServerException {
        return solrClient.addBean(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), obj,
                commitWithinMs);
    }

    public UpdateResponse addBean(Object obj, int commitWithinMs) throws IOException, SolrServerException {
        return solrClient.addBean(obj, commitWithinMs);
    }

    public UpdateResponse addBeans(String collection, Collection<?> beans) throws SolrServerException, IOException {
        return solrClient.addBeans(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), beans);
    }

    public UpdateResponse addBeans(Collection<?> beans) throws SolrServerException, IOException {
        return solrClient.addBeans(beans);
    }

    public UpdateResponse addBeans(String collection, Collection<?> beans, int commitWithinMs)
            throws SolrServerException, IOException {
        return solrClient.addBeans(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), beans,
                commitWithinMs);
    }

    public UpdateResponse addBeans(Collection<?> beans, int commitWithinMs) throws SolrServerException, IOException {
        return solrClient.addBeans(beans, commitWithinMs);
    }

    public UpdateResponse addBeans(String collection, final Iterator<?> beanIterator)
            throws SolrServerException, IOException {
        return solrClient.addBeans(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), beanIterator);
    }

    public UpdateResponse addBeans(Iterator<?> beanIterator) throws SolrServerException, IOException {
        return solrClient.addBeans(beanIterator);
    }

    public UpdateResponse commit(String collection) throws SolrServerException, IOException {
        return solrClient.commit(SolrTableUtils.getCollectionNameWithDomainName(domain, collection));
    }

    public UpdateResponse commit() throws SolrServerException, IOException {
        return solrClient.commit();
    }

    public UpdateResponse commit(String collection, boolean waitFlush, boolean waitSearcher)
            throws SolrServerException, IOException {
        return solrClient.commit(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), waitFlush,
                waitSearcher);
    }

    public UpdateResponse commit(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
        return solrClient.commit(waitFlush, waitSearcher);
    }

    public UpdateResponse commit(String collection, boolean waitFlush, boolean waitSearcher, boolean softCommit)
            throws SolrServerException, IOException {
        return solrClient.commit(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), waitFlush,
                waitSearcher, softCommit);
    }

    public UpdateResponse commit(boolean waitFlush, boolean waitSearcher, boolean softCommit)
            throws SolrServerException, IOException {
        return solrClient.commit(waitFlush, waitSearcher, softCommit);
    }

    public UpdateResponse optimize(String collection) throws SolrServerException, IOException {
        return solrClient.optimize(SolrTableUtils.getCollectionNameWithDomainName(domain, collection));
    }

    public UpdateResponse optimize() throws SolrServerException, IOException {
        return solrClient.optimize();
    }

    public UpdateResponse optimize(String collection, boolean waitFlush, boolean waitSearcher)
            throws SolrServerException, IOException {
        return solrClient.optimize(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), waitFlush,
                waitSearcher);
    }

    public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
        return solrClient.optimize(waitFlush, waitSearcher);
    }

    public UpdateResponse optimize(String collection, boolean waitFlush, boolean waitSearcher, int maxSegments)
            throws SolrServerException, IOException {
        return solrClient.optimize(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), waitFlush,
                waitSearcher, maxSegments);
    }

    public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher, int maxSegments)
            throws SolrServerException, IOException {
        return solrClient.optimize(waitFlush, waitSearcher, maxSegments);
    }

    public UpdateResponse rollback(String collection) throws SolrServerException, IOException {
        return solrClient.rollback(SolrTableUtils.getCollectionNameWithDomainName(domain, collection));
    }

    public UpdateResponse rollback() throws SolrServerException, IOException {
        return solrClient.rollback();
    }

    public UpdateResponse deleteById(String collection, String id) throws SolrServerException, IOException {
        return solrClient.deleteById(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), id);
    }

    public UpdateResponse deleteById(String id) throws SolrServerException, IOException {
        return solrClient.deleteById(id);
    }

    public UpdateResponse deleteById(String collection, String id, int commitWithinMs)
            throws SolrServerException, IOException {
        return solrClient.deleteById(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), id,
                commitWithinMs);
    }

    public UpdateResponse deleteById(String id, int commitWithinMs) throws SolrServerException, IOException {
        return solrClient.deleteById(id, commitWithinMs);
    }

    public UpdateResponse deleteById(String collection, List<String> ids) throws SolrServerException, IOException {
        return solrClient.deleteById(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), ids);
    }

    public UpdateResponse deleteById(List<String> ids) throws SolrServerException, IOException {
        return solrClient.deleteById(ids);
    }

    public UpdateResponse deleteById(String collection, List<String> ids, int commitWithinMs)
            throws SolrServerException, IOException {
        return solrClient.deleteById(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), ids,
                commitWithinMs);
    }

    public UpdateResponse deleteById(List<String> ids, int commitWithinMs) throws SolrServerException, IOException {
        return solrClient.deleteById(ids, commitWithinMs);
    }

    public UpdateResponse deleteByQuery(String collection, String query) throws SolrServerException, IOException {
        return solrClient.deleteByQuery(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), query);
    }

    public UpdateResponse deleteByQuery(String query) throws SolrServerException, IOException {
        return solrClient.deleteByQuery(query);
    }

    public UpdateResponse deleteByQuery(String collection, String query, int commitWithinMs)
            throws SolrServerException, IOException {
        return solrClient.deleteByQuery(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), query,
                commitWithinMs);
    }

    public UpdateResponse deleteByQuery(String query, int commitWithinMs) throws SolrServerException, IOException {
        return solrClient.deleteByQuery(query, commitWithinMs);
    }

    public SolrPingResponse ping() throws SolrServerException, IOException {
        return solrClient.ping();
    }

    public QueryResponse query(String collection, SolrParams params) throws SolrServerException, IOException {
        return solrClient.query(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), params);
    }

    public QueryResponse query(SolrParams params) throws SolrServerException, IOException {
        return solrClient.query(params);
    }

    public QueryResponse query(String collection, SolrParams params, SolrRequest.METHOD method)
            throws SolrServerException, IOException {
        return solrClient.query(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), params, method);
    }

    public QueryResponse query(SolrParams params, SolrRequest.METHOD method) throws SolrServerException, IOException {
        return solrClient.query(params, method);
    }

    public QueryResponse queryAndStreamResponse(String collection, SolrParams params,
                                                StreamingResponseCallback callback)
            throws SolrServerException, IOException {
        return solrClient.queryAndStreamResponse(SolrTableUtils.getCollectionNameWithDomainName(domain, collection),
                params, callback);
    }

    public QueryResponse queryAndStreamResponse(SolrParams params, StreamingResponseCallback callback)
            throws SolrServerException, IOException {
        return solrClient.queryAndStreamResponse(params, callback);
    }

    public SolrDocument getById(String collection, String id) throws SolrServerException, IOException {
        return solrClient.getById(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), id);
    }

    public SolrDocument getById(String id) throws SolrServerException, IOException {
        return solrClient.getById(id);
    }

    public SolrDocument getById(String collection, String id, SolrParams params)
            throws SolrServerException, IOException {
        return solrClient.getById(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), id, params);
    }

    public SolrDocument getById(String id, SolrParams params) throws SolrServerException, IOException {
        return solrClient.getById(id, params);
    }

    public SolrDocumentList getById(String collection, Collection<String> ids) throws SolrServerException, IOException {
        return solrClient.getById(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), ids);
    }

    public SolrDocumentList getById(Collection<String> ids) throws SolrServerException, IOException {
        return solrClient.getById(ids);
    }

    public SolrDocumentList getById(String collection, Collection<String> ids, SolrParams params)
            throws SolrServerException, IOException {
        return solrClient.getById(SolrTableUtils.getCollectionNameWithDomainName(domain, collection), ids, params);
    }

    public SolrDocumentList getById(Collection<String> ids, SolrParams params) throws SolrServerException, IOException {
        return solrClient.getById(ids, params);
    }

    @Override
    public NamedList<Object> request(SolrRequest request, String collection) throws SolrServerException, IOException {
        return solrClient.request(request, SolrTableUtils.getCollectionNameWithDomainName(domain, collection));
    }

    public DocumentObjectBinder getBinder() {
        return solrClient.getBinder();
    }

    @Override
    public void close() throws IOException {
        solrClient.close();
    }
}
