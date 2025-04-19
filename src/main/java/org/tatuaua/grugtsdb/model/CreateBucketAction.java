package org.tatuaua.grugtsdb.model;

import lombok.Data;

import java.util.List;

@Data
public class CreateBucketAction {
    String bucketName;
    List<Field> fields;

    public boolean hasTimestamp() {
        return this.fields.stream()
                .anyMatch(
                        field -> field.getName().equals("timestamp")
                );
    }
}
