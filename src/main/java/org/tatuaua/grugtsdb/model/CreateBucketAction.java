package org.tatuaua.grugtsdb.model;

import lombok.Data;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
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
