package org.tatuaua.grugtsdb.model;

import lombok.Data;

import java.util.Map;

@Data
public class WriteAction {
    String bucketName;
    Map<String, Object> fieldValues;

    public boolean hasValidTimestamp() {
        long timestamp = (long) fieldValues.get("timestamp");
        return timestamp < System.currentTimeMillis();
    }
}
