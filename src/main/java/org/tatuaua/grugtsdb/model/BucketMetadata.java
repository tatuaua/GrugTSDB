package org.tatuaua.grugtsdb.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.DataOutputStream;
import java.io.RandomAccessFile;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BucketMetadata {

    @JsonIgnore
    DataOutputStream dos;

    @JsonIgnore
    RandomAccessFile raf;

    @JsonIgnore
    long recordSize;

    long recordAmount;

    String name;

    List<Field> fields;
}
