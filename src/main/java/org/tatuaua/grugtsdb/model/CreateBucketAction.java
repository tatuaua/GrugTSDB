package org.tatuaua.grugtsdb.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;
import java.util.Map;

public class CreateBucketAction {

    @JsonIgnore
    GrugActionType actionType;

    String bucketName;

    List<GrugField> fields;
}
