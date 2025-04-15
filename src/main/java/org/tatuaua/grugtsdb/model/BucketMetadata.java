package org.tatuaua.grugtsdb.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.DataOutputStream;
import java.io.RandomAccessFile;
import java.util.List;

@Data
@AllArgsConstructor
public class BucketMetadata {

    @JsonIgnore
    DataOutputStream dos;

    @JsonIgnore
    RandomAccessFile raf;

    @JsonIgnore
    long recordSize;

    @JsonIgnore
    long recordAmount;

    List<Field> fields;
}
