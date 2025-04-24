package org.tatuaua.grugtsdb.server.model;

import lombok.Data;

import java.util.List;

import org.tatuaua.grugtsdb.engine.model.Field;

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
