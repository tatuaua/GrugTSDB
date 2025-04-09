package org.tatuaua.grugtsdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.tatuaua.grugtsdb.model.GrugBucketMetadata;
import org.tatuaua.grugtsdb.model.GrugField;

import java.io.*;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

public class IO {
    private static final File DIR = new File("grug_tsdb");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        try {
            if (DIR.exists()) {
                Files.walk(DIR.toPath())
                        .filter(Files::isRegularFile)
                        .map(java.nio.file.Path::toFile)
                        .forEach(File::delete);
                Files.deleteIfExists(DIR.toPath());
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

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(bucketFile))) {
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
                        dos.writeBytes(new String((byte[]) fieldValues.get(field.getName())));
                        break;
                    default:
                        throw new IOException("Unsupported field type: " + field.getType());
                }
            }
        }
    }

    public static void readBucketCombined(String bucketName) throws IOException {
        File bucketFile = new File(DIR, bucketName + ".grug");
        try (DataInputStream dis = new DataInputStream(new FileInputStream(bucketFile))) {
            int intValue = dis.readInt();
            System.out.println("Integer value: " + intValue);

            boolean boolValue = dis.readBoolean();
            System.out.println("Boolean value: " + boolValue);

            String stringValue = Utils.byteArray256ToString(dis.readNBytes(256));
            System.out.println("String value: " + stringValue);

            float floatValue = dis.readFloat();
            System.out.println("Float value: " + floatValue);
        } catch (IOException e) {
            throw new IOException("Error reading bucket '" + bucketName + "': " + e.getMessage(), e);
        }
    }
}
