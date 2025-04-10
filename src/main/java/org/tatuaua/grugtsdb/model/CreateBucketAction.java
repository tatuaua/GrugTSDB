package org.tatuaua.grugtsdb.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.util.List;

@Data
public class CreateBucketAction implements Action {

    @JsonIgnore
    GrugActionType actionType;

    String bucketName;

    List<GrugField> fields;
}
