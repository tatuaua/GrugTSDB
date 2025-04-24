package org.tatuaua.grugtsdb.engine.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Field {
    String name;
    FieldType type;
    int size;
}

