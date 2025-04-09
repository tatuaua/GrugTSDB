package org.tatuaua.grugtsdb.model;

import lombok.Data;

@Data
public class GrugField {
    String name;
    GrugFieldType type;
    int size;
}

