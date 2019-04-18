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

package org.wso2.extension.siddhi.store.solr;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.table.record.AbstractRecordTable;
import io.siddhi.core.table.record.ExpressionBuilder;
import io.siddhi.core.table.record.RecordIterator;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledExpression;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.util.AnnotationHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.wso2.extension.siddhi.store.solr.beans.SiddhiSolrDocument;
import org.wso2.extension.siddhi.store.solr.beans.SolrSchema;
import org.wso2.extension.siddhi.store.solr.beans.SolrSchemaField;
import org.wso2.extension.siddhi.store.solr.config.CollectionConfiguration;
import org.wso2.extension.siddhi.store.solr.exceptions.SolrClientServiceException;
import org.wso2.extension.siddhi.store.solr.exceptions.SolrTableException;
import org.wso2.extension.siddhi.store.solr.impl.SolrClientServiceImpl;
import org.wso2.extension.siddhi.store.solr.utils.SolrTableConstants;
import org.wso2.extension.siddhi.store.solr.utils.SolrTableUtils;

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
                        type = {DataType.STRING}, optional = true, defaultValue = "SolrTable_Id"),
                @Parameter(name = "zookeper.url",
                        description = "The zookeeper url of the solr cloud",
                        type = {DataType.STRING}, optional = true, defaultValue = "localhost:9983"),
                @Parameter(name = "shards",
                        description = "The number of shards of the solr collection",
                        type = {DataType.INT}, optional = true, defaultValue = "2"),
                @Parameter(name = "replicas",
                        description = "The number of replicas of the solr collection.",
                        type = {DataType.INT}, optional = true, defaultValue = "1"),
                @Parameter(name = "schema",
                        description = "The explicit solr collection schema definition.",
                        type = {DataType.STRING}, optional = true, defaultValue = "SolrTable_Schema"),
                @Parameter(name = "commit.async",
                        description = "The explicit solr collection schema definition.",
                        type = {DataType.BOOL}, optional = true, defaultValue = "true"),
                @Parameter(name = "base.config",
                        description = "The basic configset used to create the collection specific configurations.",
                        type = {DataType.STRING}, optional = true, defaultValue = "Solr_Base_Config"),
                @Parameter(name = "merge.schema",
                        description = "The basic configset used to create the collection specific configurations.",
                        type = {DataType.BOOL}, optional = true, defaultValue = "true")
        },
        examples = {
                @Example(
                        syntax = "@store(type='solr', zookeeper.url='localhost:9983', collection='TEST1', base" +
                                ".config='gettingstarted', " +
                                "shards='2', replicas='2', schema='time long stored, date string stored', " +
                                "commit.async='true')" +
                                "define table Footable(time long, date string);",
                        description = "Above example will create a solr collection which has two shards with two " +
                                "replicas which is named TEST1, using the basic config 'gettingstarted'. it will " +
                                "have two fields time and date. both fields will be indexed and stored in solr. all " +
                                "the inserts will be committed asynchronously from the solr server side")
        }
)

public class SolrTable extends AbstractRecordTable {

    private static final String SET_MODIFIER = "set";
    private static final Log log = LogFactory.getLog(SolrTable.class);
    private SolrClientServiceImpl solrClientService;
    private List<Attribute> attributes;
    private CollectionConfiguration collectionConfig;
    private List<String> primaryKeys;
    private boolean commitAsync;
    private boolean mergeSchema;
    private int readBatchSize;
    private int updateBatchSize;
    private SolrSchema solrSchema;
    private boolean schemaUpdatedOnce;
    private boolean connectedOnce;

    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        this.attributes = tableDefinition.getAttributeList();
        this.schemaUpdatedOnce = false;
        this.connectedOnce = false;
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
            String collection = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_COLLECTION);
            String url = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_URL);
            String shards = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_SHARDS);
            String replicas = storeAnnotation.getElement(SolrTableConstants
                    .ANNOTATION_ELEMENT_REPLICAS);
            String schema = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_SCHEMA);
            String configSet = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_CONFIGSET);
            String commitAsync = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_COMMIT_ASYNC);
            String mergeSchema = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_MERGE_SCHEMA);


            if (collection == null || collection.trim().isEmpty()) {
                collection = tableDefinition.getId();
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
                this.mergeSchema = true;
            }
            if (configSet == null || configSet.isEmpty()) {
                configSet = configReader.readConfig(SolrTableConstants.ANNOTATION_ELEMENT_CONFIGSET,
                        SolrTableConstants.DEFAULT_SOLR_BASE_CONFIG_NAME);
            }
            this.readBatchSize = Integer.parseInt(configReader.readConfig(SolrTableConstants
                    .PROPERTY_READ_BATCH_SIZE, SolrTableConstants.DEFAULT_READ_ITERATOR_BATCH_SIZE));
            this.updateBatchSize = Integer.parseInt(configReader.readConfig(SolrTableConstants
                    .PROPERTY_UPDATE_BATCH_SIZE, SolrTableConstants.DEFAULT_UPDATE_BATCH_SIZE));
            String domainName = configReader.readConfig(SolrTableConstants.PROPERTY_DOMAIN_IDENTIFIER,
                    SolrTableConstants.DEFAULT_PROPERTY_DOMAIN_IDENTIFIER);
            this.solrSchema = SolrTableUtils.createIndexSchema(schema);
            this.collectionConfig = new CollectionConfiguration.Builder().collectionName
                    (collection).solrServerUrl(url).shards(Integer.parseInt(shards)).replicas(Integer.parseInt
                    (replicas)).configSet(configSet).schema(this.solrSchema).domainName(domainName).build();
            this.solrClientService = SolrClientServiceImpl.INSTANCE;
        }
    }

    @Override
    protected void add(List<Object[]> records) {
        List<SiddhiSolrDocument> siddhiSolrDocuments = SolrTableUtils.createSolrDocuments(attributes, primaryKeys,
                records);
        try {
            solrClientService.insertDocuments(collectionConfig.getCollectionName(), siddhiSolrDocuments,
                    commitAsync);
        } catch (SolrClientServiceException | SolrException e) {
            log.error("Error while inserting records to Solr Event Table: " + e.getMessage(), e);
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
                    findConditionParameterMap, collectionConfig.getCollectionName());
            return new SolrRecordIterator(condition, solrClientService, collectionConfig, readBatchSize,
                    attributes);
        } catch (SolrClientServiceException | SolrException e) {
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
                        deleteConditionParameterMap, collectionConfig.getCollectionName());
                solrClientService.deleteDocuments(collectionConfig.getCollectionName(), condition, commitAsync);
            }
        } catch (SolrClientServiceException | SolrException e) {
            log.error("Error while deleting documents from Solr Event Table: " + e.getMessage(), e);
        }
    }

    @Override
    protected void update(CompiledCondition updateCondition,
                          List<Map<String, Object>> updateConditionParameterMaps,
                          Map<String, CompiledExpression> updateSetCompiledExpressionMap,
                          List<Map<String, Object>> updateSetParameterMaps) throws ConnectionUnavailableException {
        try {
            upsertSolrDocuments(updateConditionParameterMaps, updateCondition, updateSetParameterMaps,
                    updateSetCompiledExpressionMap, null);
        } catch (SolrClientServiceException | SolrServerException | IOException | SolrException e) {
            log.error("Error while searching records for updating: " + e.getMessage(), e);
        }
    }

    private void upsertSolrDocuments(List<Map<String, Object>> updateConditionParameterMaps,
                                     CompiledCondition compiledCondition,
                                     List<Map<String, Object>> updateSetParameterMaps,
                                     Map<String, CompiledExpression> updateSetCompiledExpressionMap,
                                     List<Object[]> addingRecords)
            throws SolrClientServiceException, SolrServerException, IOException {
        List<SiddhiSolrDocument> addDocs = new ArrayList<>();
        for (int index = 0; index < updateConditionParameterMaps.size(); index++) {
            Map<String, Object> updateConditionParameterMap = updateConditionParameterMaps.get(index);
            SolrRecordIterator solrRecordIterator = findRecords(updateConditionParameterMap, compiledCondition);
            if (solrRecordIterator.hasNext()) {
                List<String> deleteDocIds = new ArrayList<>();
                List<SiddhiSolrDocument> updateDocs = new ArrayList<>();
                Map<String, Object> updateSetParameterMap = updateSetParameterMaps.get(index);
                Map<String, Object> updateFields = new HashMap<>();
                for (Map.Entry<String, CompiledExpression> entry : updateSetCompiledExpressionMap.entrySet()) {
                    updateFields.put(entry.getKey(),
                            SolrTableUtils.resolveCondition((SolrCompiledCondition) entry.getValue(),
                                    updateSetParameterMap, collectionConfig.getCollectionName()));
                }
                Collection<String> updatablePrimaryKeys = updateFields.keySet();
                if (primaryKeys != null && !primaryKeys.isEmpty()) {
                    updatablePrimaryKeys.retainAll(primaryKeys);
                }
                while (solrRecordIterator.hasNext()) {
                    SiddhiSolrDocument inputDocument = new SiddhiSolrDocument();
                    SolrDocument document = solrRecordIterator.nextDocument();
                    addUpdateFieldsToSolrDocument(updateFields, inputDocument);
                    if (!updatablePrimaryKeys.isEmpty() && primaryKeys != null && !primaryKeys.isEmpty()) {
                        deleteDocIds.add(inputDocument.getFieldValue(SolrSchemaField.FIELD_ID).toString());
                        inputDocument.setField(SolrSchemaField.FIELD_ID,
                                SolrTableUtils.generateRecordIdFromPrimaryKeyValues(inputDocument, primaryKeys));

                    } else {
                        inputDocument.setField(SolrSchemaField.FIELD_ID,
                                document.getFieldValue(SolrSchemaField.FIELD_ID));
                    }
                    updateDocs.add(inputDocument);
                    if (updateDocs.size() == updateBatchSize) {
                        solrClientService.insertDocuments(collectionConfig.getCollectionName(), updateDocs,
                                commitAsync);
                        solrClientService.deleteDocuments(collectionConfig.getCollectionName(), deleteDocIds,
                                commitAsync);
                        updateDocs = new ArrayList<>();
                        deleteDocIds = new ArrayList<>();
                    }
                }
                if (!deleteDocIds.isEmpty()) {
                    solrClientService.deleteDocuments(collectionConfig.getCollectionName(), deleteDocIds, commitAsync);
                }
                if (!updateDocs.isEmpty()) {
                    solrClientService.insertDocuments(collectionConfig.getCollectionName(), updateDocs, commitAsync);
                }
            } else {
                addDocs = getNewSolrDocuments(addingRecords, index);
            }
            if (!addDocs.isEmpty()) {
                solrClientService.insertDocuments(collectionConfig.getCollectionName(), addDocs, commitAsync);
            }
        }
    }

    private void addUpdateFieldsToSolrDocument(Map<String, Object> updateFields, SiddhiSolrDocument inputDocument) {
        for (Map.Entry<String, Object> entry : updateFields.entrySet()) {
            Map<String, Object> update = new HashMap<>();
            update.put(SET_MODIFIER, entry.getValue());
            inputDocument.addField(entry.getKey(), update);
        }
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

    @Override
    protected void updateOrAdd(CompiledCondition updateCondition,
                               List<Map<String, Object>> updateConditionParameterMaps,
                               Map<String, CompiledExpression> updateSetCompiledExpressionMap,
                               List<Map<String, Object>> updateSetParameterMaps,
                               List<Object[]> addingRecords) throws ConnectionUnavailableException {
        try {
            upsertSolrDocuments(updateConditionParameterMaps, updateCondition,
                    updateSetParameterMaps, updateSetCompiledExpressionMap, addingRecords);
        } catch (SolrClientServiceException | SolrServerException | IOException | SolrException e) {
            log.error("Error while searching records for updating/adding: " + e.getMessage(), e);
        }

    }

    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        SolrConditionVisitor visitor = new SolrConditionVisitor();
        expressionBuilder.build(visitor);
        return new SolrCompiledCondition(visitor.returnCondition());
    }

    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {
        SolrSetExpressionVisitor visitor = new SolrSetExpressionVisitor();
        expressionBuilder.build(visitor);
        return new SolrCompiledCondition(visitor.returnExpression());
    }


    @Override
    protected void connect() throws ConnectionUnavailableException {
        try {
            if (!connectedOnce) {
                solrClientService.initCollection(collectionConfig);
                connectedOnce = true;
            }
            if (!schemaUpdatedOnce) {
                solrClientService.updateSolrSchema(collectionConfig.getCollectionName(), solrSchema, this.mergeSchema);
                schemaUpdatedOnce = true;
            }
        } catch (SolrException | SolrClientServiceException e) {
            throw new ConnectionUnavailableException("Error while initializing the solr Event table: " +
                    e.getMessage(), e);
        }
    }

    @Override
    protected void disconnect() {
        //ignore
    }

    @Override
    protected void destroy() {
        try {
            solrClientService.tryToCloseClient(collectionConfig);
        } catch (IOException e) {
            log.info("Error while trying to close the solr client for table: " +
                    collectionConfig.getCollectionName() + ", url: " + collectionConfig.getSolrServerUrl() +
                    ", error: " + e.getMessage(), e);
        }
    }
}
