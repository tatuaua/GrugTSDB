package org.tatuaua.grugtsdb.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

@Data
public class ReadAction {

    @JsonIgnore
    GrugActionType actionType;

    @JsonIgnore
    String bucketName;
}
