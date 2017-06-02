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
import org.wso2.siddhi.extensions.table.solr.beans.SolrIndexDocument;
import org.wso2.siddhi.extensions.table.solr.beans.SolrSchema;
import org.wso2.siddhi.extensions.table.solr.beans.SolrSchemaField;
import org.wso2.siddhi.extensions.table.solr.config.CollectionConfiguration;
import org.wso2.siddhi.extensions.table.solr.exceptions.SolrClientServiceException;
import org.wso2.siddhi.extensions.table.solr.exceptions.SolrTableException;
import org.wso2.siddhi.extensions.table.solr.impl.SolrClientServiceImpl;
import org.wso2.siddhi.extensions.table.solr.utils.SolrTableConstants;
import org.wso2.siddhi.extensions.table.solr.utils.SolrTableUtils;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.annotation.Element;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class contains the Event table implementation for Solr which is running in the cloud mode.
 */
@Extension(
        name = "solr",
        namespace = "store",
        description = "Using this extension the solr connection instructions can be assigned to the event table",
        parameters = {
                @Parameter(name = "collection",
                        description = "The name of the solr collection.",
                        type = {DataType.STRING}),
                @Parameter(name = "url",
                        description = "The zookeeper url of the solr cloud",
                        type = {DataType.STRING}),
                @Parameter(name = "shards",
                        description = "The number of shards of the solr collection",
                        type = {DataType.INT}),
                @Parameter(name = "replicas",
                        description = "The number of replicas of the solr collection.",
                        type = {DataType.INT}),
                @Parameter(name = "schema",
                        description = "The explicit solr collection schema definition.",
                        type = {DataType.STRING}),
                @Parameter(name = "commit.async",
                        description = "The explicit solr collection schema definition.",
                        type = {DataType.BOOL}),
                @Parameter(name = "base.config",
                        description = "The basic configset used to create the collection specific configurations.",
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax = "@store(type='solr', url='localhost:9983', collection='TEST1', base.config='gettingstarted', " +
                                 "shards='2', replicas='2', schema='time long stored, date string stored', commit.async='true') " +
                                 "define table Footable(time long, date string);",
                description = "Above example will create a solr collection which has two shards with two replicas " +
                              "which is named TEST1, using the basic config 'gettingstarted'. it will have two fields" +
                              " time and date. both fields will be indexed and stored in solr. all the inserts will " +
                              "be commited asynchronously from the solr server side")
        }
)

public class SolrTable extends AbstractRecordTable {

    private static final String ATOMIC_SET_MODIFIER = "set";
    private static final int ITERATOR_BATCH_SIZE = 1000;
    private SolrClientService solrClientService;
    private List<Attribute> attributes;
    private String collection;
    private List<String> primaryKeys;
    private boolean commitAsync;

    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        this.attributes = tableDefinition.getAttributeList();
        Annotation primaryKeyAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_PRIMARY_KEY,
                                                                         tableDefinition.getAnnotations());
        Annotation storeAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_STORE, tableDefinition
                .getAnnotations());
        if (primaryKeyAnnotation != null) {
            primaryKeys = new ArrayList<>();
            List<Element> primaryKeyElements = primaryKeyAnnotation.getElements();
            primaryKeyElements.forEach(element -> {
                primaryKeys.add(element.getValue().trim());
            });
        }

        if (storeAnnotation != null) {
            collection = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_COLLECTION);
            String url = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_URL);
            int shards = Integer.parseInt(storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_SHARDS));
            int replicas = Integer.parseInt(storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_REPLICA));
            String schema = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_SCHEMA);
            String configSet = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_CONFIGSET);
            String commitAsync = storeAnnotation.getElement(SolrTableConstants.ANNOTATION_ELEMENT_COMMIT_ASYNC);


            if (collection == null || collection.trim().isEmpty()) {
                throw new ExecutionPlanCreationException("Solr collection name cannot be null or empty");
            }
            if (url == null || url.trim().isEmpty()) {
                throw new ExecutionPlanCreationException("SolrCloud url cannot be null or empty");
            }
            if (shards < 0 || replicas < 0 ) {
                throw new ExecutionPlanCreationException("No of shards and no of replicas cannot be empty or less " +
                                                         "than 1");
            }
            if (commitAsync == null || commitAsync.isEmpty()) {
                this.commitAsync = true;
            } else {
                this.commitAsync = Boolean.parseBoolean(commitAsync);
            }
            SolrSchema solrSchema = SolrTableUtils.createIndexSchema(schema);
            CollectionConfiguration collectionConfig = new CollectionConfiguration.Builder().collectionName
                    (collection).solrServerUrl(url).shards(shards).replicas(replicas).configs(configSet).schema
                    (solrSchema).build();
            solrClientService = new SolrClientServiceImpl();
            try {
                solrClientService.createCollection(collectionConfig);
                solrClientService.updateSolrSchema(collection, solrSchema, true);
            } catch (SolrClientServiceException e) {
                throw new ExecutionPlanCreationException("Error while initializing the Solr Event table: " + e
                        .getMessage(), e);
            }
        }
    }

    @Override
    protected void add(List<Object[]> records) {
        List<SolrIndexDocument> solrIndexDocuments = SolrTableUtils.createSolrDocuments(attributes, primaryKeys,
                                                                                        records);
        try {
            solrClientService.insertDocuments(collection, solrIndexDocuments, commitAsync);
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
                                                               findConditionParameterMap);
            return new SolrRecordIterator(condition, solrClientService, collection, ITERATOR_BATCH_SIZE, attributes);
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
                                                                   deleteConditionParameterMap);
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
        List<SolrIndexDocument> updateDocs = new ArrayList<>();
        List<SolrIndexDocument> addDocs = new ArrayList<>();
        int index = 0;
        for (Map<String, Object> updateConditionParameterMap : updateConditionParameterMaps) {
            SolrRecordIterator solrRecordIterator = findRecords(updateConditionParameterMap, compiledCondition);
            Map<String, Object> updateFields = updateValues.get(index);
            if (solrRecordIterator.hasNext()) {
                while (solrRecordIterator.hasNext()) {
                    SolrIndexDocument inputDocument = new SolrIndexDocument();
                    SolrDocument document = solrRecordIterator.nextDocument();
                    inputDocument.addField(SolrSchemaField.FIELD_ID, document.getFieldValue(SolrSchemaField.FIELD_ID));
                    for (Map.Entry<String, Object> entry : updateFields.entrySet()) {
                        Map<String, Object> update = new HashMap<>();
                        update.put(ATOMIC_SET_MODIFIER, entry.getValue());
                        inputDocument.addField(entry.getKey(), update);
                    }
                    updateDocs.add(inputDocument);
                }
            } else {
                if (addingRecords != null && !addingRecords.isEmpty()) {
                    SolrIndexDocument newDoc = SolrTableUtils.createSolrDocument(attributes, primaryKeys,
                                                                                 addingRecords.get(index));
                    addDocs.add(newDoc);
                }
            }
            index++;
        }
        if (!addDocs.isEmpty()) {
            updateDocs.addAll(addDocs);
        }
        solrClientService.insertDocuments(collection, updateDocs, commitAsync);
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
