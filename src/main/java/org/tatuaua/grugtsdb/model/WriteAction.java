package org.tatuaua.grugtsdb.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.util.Map;

@Data
public class WriteAction implements Action {

    @JsonIgnore
    GrugActionType actionType;

    @JsonIgnore
    String bucketName;

    Map<String, Object> fieldValues;
}
