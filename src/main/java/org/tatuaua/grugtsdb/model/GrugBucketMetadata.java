package org.tatuaua.grugtsdb.model;

import lombok.Data;

import java.util.List;

@Data
public class GrugBucketMetadata {
    List<GrugField> fields;
}
