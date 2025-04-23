package org.tatuaua.grugtsdb.model;

import lombok.Data;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateStreamAction {
    String bucketName;
    List<String> bucketsToStream;
}
