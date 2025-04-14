package org.tatuaua.grugtsdb.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.util.Map;

@Data
public class WriteAction {

    @JsonIgnore
    GrugActionType actionType;

    String bucketName;

    Map<String, Object> fieldValues;
}
