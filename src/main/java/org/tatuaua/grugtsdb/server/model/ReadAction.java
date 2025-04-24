package org.tatuaua.grugtsdb.server.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ReadAction {
    String bucketName;
    ReadActionType type;
}
