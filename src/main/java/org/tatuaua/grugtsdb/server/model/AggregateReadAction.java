package org.tatuaua.grugtsdb.server.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class AggregateReadAction {
    String bucketName;
    String fieldName;
    String aggregationType;
    long timeRangeStart;
    long timeRangeEnd;
}
