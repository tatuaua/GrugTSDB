package org.tatuaua.grugtsdb.model;

import lombok.Data;

import java.util.List;

@Data
public class CreateStreamAction {
    String bucketName;
    List<String> bucketsToStream;
}
