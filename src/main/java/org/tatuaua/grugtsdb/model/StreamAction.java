package org.tatuaua.grugtsdb.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonValueInstantiator;

import java.util.List;

public class StreamAction {

    @JsonIgnore
    ActionType actionType;

    String bucketName;

    List<String> bucketsToStream;

    List<String> bucketsToFilter;
}
