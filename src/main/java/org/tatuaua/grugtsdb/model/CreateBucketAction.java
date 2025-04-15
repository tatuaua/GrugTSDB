package org.tatuaua.grugtsdb.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.util.List;

@Data
public class CreateBucketAction {

    @JsonIgnore
    ActionType actionType;

    String bucketName;

    List<Field> fields;
}
