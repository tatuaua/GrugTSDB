package org.tatuaua.grugtsdb.model;

import lombok.Data;

@Data
public class Field {
    String name;
    FieldType type;
    int size;
}

