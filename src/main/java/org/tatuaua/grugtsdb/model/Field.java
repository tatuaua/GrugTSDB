package org.tatuaua.grugtsdb.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Field {
    String name;
    FieldType type;
    int size;
}

