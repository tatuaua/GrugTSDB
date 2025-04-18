package org.tatuaua.grugtsdb.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.util.List;

@Data
public class CreateStreamAction {
    String bucketName;
    List<String> bucketsToStream;
}
