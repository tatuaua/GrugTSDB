package org.tatuaua.grugtsdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.tatuaua.grugtsdb.model.GrugBucketMetadata;
import org.tatuaua.grugtsdb.model.GrugField;
import org.tatuaua.grugtsdb.model.GrugFieldType;

import org.apache.commons.io.FileUtils;
import org.tatuaua.grugtsdb.model.GrugReadResponse;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DB {
    private static final File DIR = new File("grug_tsdb");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        try {
            if (DIR.exists()) {
                FileUtils.deleteDirectory(DIR);
            }
            Files.createDirectories(DIR.toPath());
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize database directory", e);
        }
    }

    public static void createBucket(String bucketName, List<GrugField> fields) throws IOException {
        File bucketFile = new File(DIR, bucketName + ".grug");
        if (!bucketFile.createNewFile()) {
            throw new IOException("Bucket '" + bucketName + "' already exists");
        }

        GrugBucketMetadata metadata = new GrugBucketMetadata();
        metadata.setFields(fields);
        createBucketMetadata(bucketName, metadata);
    }

    private static void createBucketMetadata(String bucketName, GrugBucketMetadata metadata) throws IOException {
        File metadataFile = new File(DIR, bucketName + "_meta.json");
        if (!metadataFile.createNewFile()) {
            throw new IOException("Metadata for bucket '" + bucketName + "' already exists");
        }

        try (FileOutputStream fos = new FileOutputStream(metadataFile)) {
            String metadataJson = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(metadata);
            fos.write(metadataJson.getBytes());
        }
    }

    public static GrugBucketMetadata readBucketMetadata(String bucketName) throws IOException {
        File metadataFile = new File(DIR, bucketName + "_meta.json");
        try (InputStreamReader reader = new InputStreamReader(new FileInputStream(metadataFile))) {
            return MAPPER.readValue(reader, GrugBucketMetadata.class);
        }
    }

    public static void writeToBucket(String bucketName, Map<String, Object> fieldValues) throws IOException {
        GrugBucketMetadata metadata = readBucketMetadata(bucketName);
        File bucketFile = new File(DIR, bucketName + ".grug");

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(bucketFile, true))) {
            for (GrugField field : metadata.getFields()) {
                Object value = fieldValues.get(field.getName());
                if (value == null) {
                    throw new IOException("Missing required field: " + field.getName());
                }

                switch (field.getType()) {
                    case INT:
                        dos.writeInt((int) value);
                        break;
                    case BOOLEAN:
                        dos.writeBoolean((boolean) value);
                        break;
                    case FLOAT:
                        dos.writeFloat((float) value);
                        break;
                    case STRING:
                        dos.writeBytes(new String(Utils.stringToByteArray((String) value, field.getSize())));
                        break;
                    default:
                        throw new IOException("Unsupported field type: " + field.getType());
                }
            }
        }
    }

    public static GrugReadResponse readFullBucket(String bucketName) throws IOException {
        GrugBucketMetadata metadata = readBucketMetadata(bucketName);
        List<GrugField> fields = metadata.getFields();
        List<Map<String, Object>> data = new ArrayList<>();
        File bucketFile = new File(DIR, bucketName + ".grug");
        try (DataInputStream dis = new DataInputStream(new FileInputStream(bucketFile))) {
            while (dis.available() > 0) {
                Map<String, Object> record = new HashMap<>();
                for (GrugField field : fields) {
                    if (field.getType() == GrugFieldType.INT) {
                        record.put(field.getName(), dis.readInt());
                    } else if (field.getType() == GrugFieldType.BOOLEAN) {
                        record.put(field.getName(), dis.readBoolean());
                    } else if (field.getType() == GrugFieldType.FLOAT) {
                        record.put(field.getName(), dis.readFloat());
                    } else if (field.getType() == GrugFieldType.STRING) {
                        record.put(field.getName(), Utils.byteArrayToString(dis.readNBytes(field.getSize())));
                    }
                }
                data.add(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new IOException("Error reading bucket '" + bucketName + "': ");
        }
        GrugReadResponse response = new GrugReadResponse();
        response.setCount(data.size());
        response.setData(data);
        return response;
    }
}
