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

package org.wso2.siddhi.extensions.store.solr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.table.record.AbstractRecordTable;
import org.wso2.siddhi.core.table.record.ConditionBuilder;
import org.wso2.siddhi.core.table.record.RecordIterator;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.extensions.store.solr.beans.SiddhiSolrDocument;
import org.wso2.siddhi.extensions.store.solr.beans.SolrSchema;
import org.wso2.siddhi.extensions.store.solr.beans.SolrSchemaField;
import org.wso2.siddhi.extensions.store.solr.config.CollectionConfiguration;
import org.wso2.siddhi.extensions.store.solr.exceptions.SolrClientServiceException;
import org.wso2.siddhi.extensions.store.solr.exceptions.SolrTableException;
import org.wso2.siddhi.extensions.store.solr.impl.SolrClientServiceImpl;
import org.wso2.siddhi.extensions.store.solr.utils.SolrTableConstants;
import org.wso2.siddhi.extensions.store.solr.utils.SolrTableUtils;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.annotation.Element;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class contains the Event table implementation for Solr which is running in the cloud mode.
 */
@Extension(
        name = "solr",
        namespace = "store",
        description = "Solr store implementation uses solr collections for underlying data storage. The events are " +
                      "converted to Solr documents when the events are inserted to solr store. Solr documents are " +
                      "converted to Events when the Solr documents are read from solr collections. This can only be " +
                      "used with the Solr cloud mode.",
        parameters = {
                @Parameter(name = "collection",
                        description = "The name of the solr collection.",
                        type = {DataType.STRING}, optional = true),
                @Parameter(name = "zookeper.url",
                        description = "The zookeeper url of the solr cloud",
                        type = {DataType.STRING}, optional = true),
                @Parameter(name = "shards",
                        description = "The number of shards of the solr collection",
                        type = {DataType.INT}, optional = true),
                @Parameter(name = "replicas",
                        description = "The number of replicas of the solr collection.",
                        type = {DataType.INT}, optional = true),
                @Parameter(name = "schema",
                        description = "The explicit solr collection schema definition.",
                        type = {DataType.STRING}, optional = true),
                @Parameter(name = "commit.async",
                        description = "The explicit solr collection schema definition.",
                        type = {DataType.BOOL}, optional = true),
                @Parameter(name = "base.config",
                        description = "The basic configset used to create the collection specific configurations.",
                        type = {DataType.STRING}, optional = true),
                @Parameter(name = "merge.schema",
                        description = "The basic configset used to create the collection specific configurations.",
                        type = {DataType.BOOL}, optional = true)
        },
        examples = {
                @Example(
                        syntax = "@store(type='solr', zookeeper.url='localhost:9983', collection='TEST1', base" +
                                 ".config='gettingstarted', " +
                                 "shards='2', replicas='2', schema='time long stored, date string stored', " +
                                 "commit.async='true')" +
                                 "define table Footable(time long, date string);",
                description = "Above example will create a solr collection which has two shards with two replicas " +
                              "which is named TEST1, using the basic config 'gettingstarted'. it will have two fields" +
                              " time and date. both fields will be indexed and stored in solr. all the inserts will " +
                              "be commited asynchronously from the solr server side")
        }
)

public class SolrTable extends AbstractRecordTable {

    private static final String SET_MODIFIER = "set";
    private static final Log log = LogFactory.getLog(SolrTable.class);
    private SolrClientServiceImpl solrClientService;
    private List<Attribute> attributes;
    private String collection;
    private List<String> primaryKeys;
    private boolean commitAsync;
    private boolean mergeSchema;
    private int readBatchSize;

    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        this.attributes = tableDefinition.getAttributeList();
        Annotation primaryKeyAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_PRIMARY_KEY,
                                                                         tableDefinition.getAnnotations());
        Annotation storeAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_STORE, tableDefinition
                .getAnnotations());
        if (primaryKeyAnnotation != null) {
            this.primaryKeys = new ArrayList<>();
            List<Element> primaryKeyElements = primaryKeyAnnotation.getElements();
            primaryKeyElements.forEach(element -> {
                this.primaryKeys.add(element.getValue().trim());
            });
        }
        if (storeAnnotation != null) {
            this.collection = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_COLLECTION);
            String url = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_URL);
            String shards = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_SHARDS);
            String replicas = storeAnnotation.getElement(SolrTableConstants
                                                                 .ANNOTATION_ELEMENT_REPLICAS);
            String schema = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_SCHEMA);
            String configSet = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_CONFIGSET);
            String commitAsync = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_COMMIT_ASYNC);
            String mergeSchema = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_MERGE_SCHEMA);


            if (this.collection == null || this.collection.trim().isEmpty()) {
                this.collection = tableDefinition.getId();
            }
            if (url == null || url.trim().isEmpty()) {
                url = configReader.readConfig(SolrTableConstants.ANNOTATION_ELEMENT_URL, SolrTableConstants
                        .DEFAULT_ZOOKEEPER_URL);
            }
            if (shards == null) {
                shards = configReader.readConfig(SolrTableConstants.ANNOTATION_ELEMENT_SHARDS, SolrTableConstants
                        .DEFAULT_SHARD_COUNT);
            }
            if (replicas == null) {
                replicas = configReader.readConfig(SolrTableConstants.ANNOTATION_ELEMENT_REPLICAS, SolrTableConstants
                        .DEFAULT_REPLICAS_COUNT);
            }
            if (commitAsync == null || commitAsync.isEmpty()) {
                this.commitAsync = true;
            } else {
                this.commitAsync = Boolean.parseBoolean(commitAsync);
            }
            if (mergeSchema != null && !mergeSchema.isEmpty()) {
                this.mergeSchema = Boolean.parseBoolean(mergeSchema);
            } else {
                this.mergeSchema = false;
            }
            this.readBatchSize = Integer.parseInt(configReader.readConfig(SolrTableConstants
                    .PROPERTY_READ_BATCH_SIZE, SolrTableConstants.DEFAULT_READ_ITERATOR_BATCH_SIZE));
            SolrSchema solrSchema = SolrTableUtils.createIndexSchema(schema);
            CollectionConfiguration collectionConfig = new CollectionConfiguration.Builder().collectionName
                    (this.collection).solrServerUrl(url).shards(Integer.parseInt(shards)).replicas(Integer.parseInt
                    (replicas)).configs
                    (configSet).schema
                    (solrSchema).build();
            solrClientService = SolrClientServiceImpl.getInstance();
            try {
                solrClientService.createCollection(collectionConfig);
                solrClientService.updateSolrSchema(this.collection, solrSchema, this.mergeSchema);
            } catch (SolrClientServiceException e) {
                log.error("Error while initializing the Solr Event table: " + e.getMessage(), e);
                throw new ExecutionPlanCreationException("Error while initializing the Solr Event table: " + e
                        .getMessage(), e);
            }
        }
    }

    @Override
    protected void add(List<Object[]> records) {
        List<SiddhiSolrDocument> siddhiSolrDocuments = SolrTableUtils.createSolrDocuments(attributes, primaryKeys,
                                                                                        records);
        try {
            solrClientService.insertDocuments(collection, siddhiSolrDocuments, commitAsync);
        } catch (SolrClientServiceException e) {
            throw new SolrTableException("Error while inserting records to Solr Event Table: " + e.getMessage(), e);
        }
    }

    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap, CompiledCondition
            compiledCondition) {
        return findRecords(findConditionParameterMap, (SolrCompiledCondition) compiledCondition);
    }

    private SolrRecordIterator findRecords(Map<String, Object> findConditionParameterMap, CompiledCondition
            compiledCondition) {
        try {
            String condition = SolrTableUtils.resolveCondition((SolrCompiledCondition) compiledCondition,
                                                               findConditionParameterMap, collection);
            return new SolrRecordIterator(condition, solrClientService, collection, readBatchSize, attributes);
        } catch (SolrClientServiceException e) {
            throw new SolrTableException("Error while searching records in Solr Event Table: " + e.getMessage(), e);
        }
    }

    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap, CompiledCondition compiledCondition) {
        RecordIterator iterator = findRecords(containsConditionParameterMap, compiledCondition);
        return iterator.hasNext();
    }

    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition) {
        try {
            for (Map<String, Object> deleteConditionParameterMap : deleteConditionParameterMaps) {
                String condition = SolrTableUtils.resolveCondition((SolrCompiledCondition) compiledCondition,
                                                                   deleteConditionParameterMap, collection);
                solrClientService.deleteDocuments(collection, condition, commitAsync);
            }
        } catch (SolrClientServiceException e) {
            throw new SolrTableException("Error while deleting documents from Solr Event Table: " + e.getMessage(), e);
        }
    }

    @Override
    protected void update(List<Map<String, Object>> updateConditionParameterMaps, CompiledCondition compiledCondition,
                          List<Map<String, Object>> updateValues) {
        try {
            upsertSolrDocuments(updateConditionParameterMaps, compiledCondition, updateValues, null);
        } catch (SolrClientServiceException | SolrServerException | IOException e) {
            throw new SolrTableException("Error while searching records for updating: " + e.getMessage(), e);
        }
    }

    private void upsertSolrDocuments(List<Map<String, Object>> updateConditionParameterMaps,
                                     CompiledCondition compiledCondition, List<Map<String, Object>> updateValues,
                                     List<Object[]> addingRecords)
            throws SolrClientServiceException, SolrServerException, IOException {
        List<SiddhiSolrDocument> updateDocs = new ArrayList<>();
        List<SiddhiSolrDocument> addDocs = new ArrayList<>();
        List<String> deleteDocIds = new ArrayList<>();

        for (int index = 0; index < updateConditionParameterMaps.size(); index++) {
            Map<String, Object> updateConditionParameterMap = updateConditionParameterMaps.get(index);
            SolrRecordIterator solrRecordIterator = findRecords(updateConditionParameterMap, compiledCondition);
            if (solrRecordIterator.hasNext()) {
                Map<String, Object> updateFields = updateValues.get(index);
                updateDocs = getUpdateDocuments(solrRecordIterator, updateFields);
            } else {
                addDocs = getNewSolrDocuments(addingRecords, index);
            }
        }
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            if (updateValues != null && !updateValues.isEmpty()) {
                Collection<String> updateFields = updateValues.get(0).keySet();
                updateFields.retainAll(primaryKeys);
                if (!updateFields.isEmpty()) {
                    for (SiddhiSolrDocument doc : updateDocs) {
                        deleteDocIds.add(doc.getFieldValue(SolrSchemaField.FIELD_ID).toString());
                        doc.setField(SolrSchemaField.FIELD_ID, SolrTableUtils.generateRecordIdFromPrimaryKeyValues
                                (doc, primaryKeys));
                    }
                }
            }
        }
        if (!addDocs.isEmpty()) {
            updateDocs.addAll(addDocs);
        }
        if (!deleteDocIds.isEmpty()) {
            solrClientService.deleteDocuments(collection, deleteDocIds, false);
        }
        solrClientService.insertDocuments(collection, updateDocs, commitAsync);
    }

    private List<SiddhiSolrDocument> getNewSolrDocuments(List<Object[]> addingRecords, int index) {
        List<SiddhiSolrDocument> addDocs = new ArrayList<>();
        if (addingRecords != null && !addingRecords.isEmpty()) {
            SiddhiSolrDocument newDoc = SolrTableUtils.createSolrDocument(attributes, primaryKeys,
                                                                         addingRecords.get(index));
            addDocs.add(newDoc);
        }
        return addDocs;
    }

    private List<SiddhiSolrDocument> getUpdateDocuments(SolrRecordIterator solrRecordIterator,
                                                       Map<String, Object> updateFields) {
        List<SiddhiSolrDocument> updateDocs = new ArrayList<>();
        while (solrRecordIterator.hasNext()) {
            SiddhiSolrDocument inputDocument = new SiddhiSolrDocument();
            SolrDocument document = solrRecordIterator.nextDocument();
            for (Map.Entry<String, Object> entry : updateFields.entrySet()) {
                Map<String, Object> update = new HashMap<>();
                update.put(SET_MODIFIER, entry.getValue());
                inputDocument.addField(entry.getKey(), update);
            }
            inputDocument.setField(SolrSchemaField.FIELD_ID, document.getFieldValue(SolrSchemaField.FIELD_ID));
            updateDocs.add(inputDocument);
        }
        return updateDocs;
    }

    @Override
    protected void updateOrAdd(List<Map<String, Object>> updateConditionParameterMaps,
                               CompiledCondition compiledCondition, List<Map<String, Object>> updateValues,
                               List<Object[]> addingRecords) {
        try {
            upsertSolrDocuments(updateConditionParameterMaps, compiledCondition, updateValues, addingRecords);
        } catch (SolrClientServiceException | SolrServerException | IOException e) {
            throw new SolrTableException("Error while searching records for updating/adding: " + e.getMessage(), e);
        }

    }

    @Override
    protected CompiledCondition compileCondition(ConditionBuilder conditionBuilder) {
        SolrConditionVisitor visitor = new SolrConditionVisitor();
        conditionBuilder.build(visitor);
        return new SolrCompiledCondition(visitor.returnCondition());
    }
}
