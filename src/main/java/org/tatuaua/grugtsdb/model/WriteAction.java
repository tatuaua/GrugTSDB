package org.tatuaua.grugtsdb.model;

import lombok.Data;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class WriteAction {
    String bucketName;
    Map<String, Object> fieldValues;

    public boolean hasValidTimestamp() {
        long timestamp = (long) fieldValues.get("timestamp");
        return timestamp < System.currentTimeMillis();
    }
}
