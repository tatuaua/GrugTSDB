package org.tatuaua.grugtsdb.model;

import lombok.Data;

@Data
public class ReadAction {
    String bucketName;
    ReadActionType type;
}
