package org.tatuaua.grugtsdb.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.DataOutputStream;
import java.util.List;

@Data
@AllArgsConstructor
public class GrugBucketMetadata {

    @JsonIgnore
    DataOutputStream dos;

    List<GrugField> fields;
}
