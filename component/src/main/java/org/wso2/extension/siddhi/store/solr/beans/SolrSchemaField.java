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

package org.wso2.extension.siddhi.store.solr.beans;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * This class represents the fields in the solr schema
 */
public class SolrSchemaField implements Serializable {

    private static final long serialVersionUID = 7243610548183777241L;
    private Map<String, Object> properties;

    /**
     * Common attributes used to describe each field
     */
    public static final String ATTR_FIELD_NAME = "name";
    public static final String ATTR_STORED = "stored"; //Can be removed because of docValues
    public static final String ATTR_TYPE = "type";

    /**
     * Fields used internally
     */
    public static final String FIELD_ID = "id";
    public static final String FIELD_VERSION = "_version_";

    public SolrSchemaField() {
        properties = new HashMap<>();
    }

    public SolrSchemaField(Map<String, Object> properties) {
        this.properties = new HashMap<>();
        if (properties != null) {
            this.properties.putAll(properties);
        }
    }


    public SolrSchemaField(SolrSchemaField solrSchemaField) {
        this();
        if (solrSchemaField != null) {
            this.properties = solrSchemaField.getProperties();
        }

    }

    public Map<String, Object> getProperties() {
        return new HashMap<>(properties);
    }

    public Object getProperty(String name) {
        return this.properties.get(name);
    }

    public void setProperties(Map<String, Object> otherProperties) {
        if (otherProperties != null) {
            this.properties.putAll(otherProperties);
        }
    }

    public void setProperty(String name, Object value) {
        if (name != null && !name.trim().isEmpty()) {
            this.properties.put(name, value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SolrSchemaField that = (SolrSchemaField) o;

        if (properties != null ? !properties.equals(that.properties) : that.properties != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return properties.hashCode();
    }
}
