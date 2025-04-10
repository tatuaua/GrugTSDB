package org.tatuaua.grugtsdb.model;

import lombok.Data;

import java.util.Map;
import java.util.List;

@Data
public class GrugReadResponse {
    int count;
    List<Map<String, Object>> data;
}
