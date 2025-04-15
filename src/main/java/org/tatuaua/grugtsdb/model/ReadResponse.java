package org.tatuaua.grugtsdb.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
@AllArgsConstructor
public class ReadResponse {
    Map<String, Object> data;
}
