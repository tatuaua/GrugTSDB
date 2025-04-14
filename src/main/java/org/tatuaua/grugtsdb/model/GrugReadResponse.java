package org.tatuaua.grugtsdb.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
@AllArgsConstructor
public class GrugReadResponse {
    Map<String, Object> data;
}
