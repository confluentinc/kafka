/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.image;

import org.apache.kafka.common.message.DescribeClientQuotasResponseData.ValueData;
import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.metadata.ClientQuotaRecord.EntityData;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.image.node.ClientQuotaImageNode;
import org.apache.kafka.image.writer.ImageWriter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


/**
 * Represents a quota for a client entity in the metadata image.
 *
 * This class is thread-safe.
 */
public record ClientQuotaImage(Map<String, Double> quotas) {
    public static final ClientQuotaImage EMPTY = new ClientQuotaImage(Map.of());

    public ClientQuotaImage {
        quotas = Collections.unmodifiableMap(quotas);
    }

    public void write(
        ClientQuotaEntity entity,
        ImageWriter writer
    ) {
        for (Entry<String, Double> entry : quotas.entrySet()) {
            writer.write(0, new ClientQuotaRecord().
                setEntity(entityToData(entity)).
                setKey(entry.getKey()).
                setValue(entry.getValue()).
                setRemove(false));
        }
    }

    public static List<EntityData> entityToData(ClientQuotaEntity entity) {
        List<EntityData> entityData = new ArrayList<>(entity.entries().size());
        for (Entry<String, String> entry : entity.entries().entrySet()) {
            entityData.add(new EntityData().
                setEntityType(entry.getKey()).
                setEntityName(entry.getValue()));
        }
        return entityData;
    }

    public static ClientQuotaEntity dataToEntity(List<EntityData> entityData) {
        Map<String, String> entries = new HashMap<>();
        for (EntityData data : entityData) {
            entries.put(data.entityType(), data.entityName());
        }
        return new ClientQuotaEntity(Collections.unmodifiableMap(entries));
    }

    public List<ValueData> toDescribeValues() {
        List<ValueData> values = new ArrayList<>(quotas.size());
        for (Entry<String, Double> entry : quotas.entrySet()) {
            values.add(new ValueData().setKey(entry.getKey()).setValue(entry.getValue()));
        }
        return values;
    }

    public boolean isEmpty() {
        return quotas.isEmpty();
    }

    @Override
    public String toString() {
        return new ClientQuotaImageNode(this).stringify();
    }
}
