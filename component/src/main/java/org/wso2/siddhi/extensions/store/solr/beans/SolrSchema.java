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

package org.wso2.siddhi.extensions.store.solr.beans;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * This class represents the Solr schema.
 */
public class SolrSchema implements Serializable {

    private static final long serialVersionUID = 4547647202218007934L;
    private String uniqueKey;
    private Map<String, SolrSchemaField> fields;

    /**
     * This constructor will only be called by the deserializer
     */
    public SolrSchema() {
        fields = new HashMap<>();
    }

    public SolrSchema(String uniqueKey, Map<String, SolrSchemaField> fields) {
        this.fields = new HashMap<>();
        if (uniqueKey == null || uniqueKey.trim().isEmpty()) {
            this.uniqueKey = SolrSchemaField.FIELD_ID;
        } else {
            this.uniqueKey = uniqueKey;
        }
        this.fields.putAll(fields);
    }

    public String getUniqueKey() {
        return uniqueKey;
    }

    public void setUniqueKey(String uniqueKey) {
        this.uniqueKey = uniqueKey;
    }

    public Map<String, SolrSchemaField> getFields() {

        return new HashMap<>(fields);
    }

    public void setFields(Map<String, SolrSchemaField> fields) {
        this.fields = new HashMap<>(fields);
    }

    public void addField(String name, SolrSchemaField solrSchemaField) {
        SolrSchemaField field = new SolrSchemaField(solrSchemaField);
        fields.put(name, field);
    }

    public void addField(SolrSchemaField solrSchemaField) {
        SolrSchemaField field = new SolrSchemaField(solrSchemaField);
        fields.put(field.getProperty(SolrSchemaField.ATTR_FIELD_NAME).toString(), field);
    }

    public SolrSchemaField getField(String fieldName) {
        if (fields.get(fieldName) != null) {
            return new SolrSchemaField(fields.get(fieldName));
        } else {
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SolrSchema)) {
            return false;
        }

        SolrSchema that = (SolrSchema) o;

        return !(fields != null ? !fields.equals(that.fields) : that.fields != null) &&
               !(uniqueKey != null ? !uniqueKey.equals(that.uniqueKey) : that.uniqueKey != null);

    }

    @Override
    public int hashCode() {
        int result = uniqueKey != null ? uniqueKey.hashCode() : 0;
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        return result;
    }
}
