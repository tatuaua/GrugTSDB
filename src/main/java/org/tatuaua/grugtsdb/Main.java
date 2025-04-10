package org.tatuaua.grugtsdb;

import org.tatuaua.grugtsdb.model.GrugField;
import org.tatuaua.grugtsdb.model.GrugFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException {

        GrugField field = new GrugField();
        field.setName("field_name");
        field.setType(GrugFieldType.INT);

        GrugField field2 = new GrugField();
        field2.setName("other");
        field2.setType(GrugFieldType.BOOLEAN);

        GrugField field3 = new GrugField();
        field3.setName("str");
        field3.setType(GrugFieldType.STRING);

        GrugField field4 = new GrugField();
        field4.setName("fl");
        field4.setType(GrugFieldType.FLOAT);

        List<GrugField> fields = new ArrayList<>();
        fields.add(field);
        fields.add(field2);
        fields.add(field3);
        fields.add(field4);

        DB.createBucket("balls", fields);

        Map<String, Object> values = new HashMap<>();
        values.put("field_name", 69);
        values.put("other", false);
        values.put("str", Utils.stringTo256ByteArray("mystr"));
        values.put("fl", 0.3f);

        DB.writeToBucket("balls", values);
    }
}