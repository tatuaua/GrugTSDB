package org.tatuaua.grugtsdb;

import org.tatuaua.grugtsdb.model.Field;
import org.tatuaua.grugtsdb.model.FieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException {

        Field field = new Field();
        field.setName("field_name");
        field.setType(FieldType.INT);

        Field field2 = new Field();
        field2.setName("other");
        field2.setType(FieldType.BOOLEAN);

        Field field3 = new Field();
        field3.setName("str");
        field3.setType(FieldType.STRING);

        Field field4 = new Field();
        field4.setName("fl");
        field4.setType(FieldType.FLOAT);

        List<Field> fields = new ArrayList<>();
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