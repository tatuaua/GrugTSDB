package org.tatuaua.grugtsdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.tatuaua.grugtsdb.model.GrugBucketMetadata;
import org.tatuaua.grugtsdb.model.GrugField;

import org.apache.commons.io.FileUtils;
import org.tatuaua.grugtsdb.model.GrugReadResponse;

import java.io.*;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DB {
    private static final File DIR = new File("grug_tsdb");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<String, GrugBucketMetadata> BUCKET_METADATA_MAP = new HashMap<>();

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

        DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(
                        new FileOutputStream(bucketFile, true), 8192 // 8KB buffer
                )
        );

        GrugBucketMetadata metadata = new GrugBucketMetadata(dos, fields);
        metadata.setFields(fields);
        writeBucketMetadata(bucketName, metadata);

        BUCKET_METADATA_MAP.put(bucketName, metadata);
    }

    private static void writeBucketMetadata(String bucketName, GrugBucketMetadata metadata) throws IOException {
        File metadataFile = new File(DIR, bucketName + "_meta.json");
        if (!metadataFile.createNewFile()) {
            throw new IOException("Metadata for bucket '" + bucketName + "' already exists");
        }

        try (FileOutputStream fos = new FileOutputStream(metadataFile)) {
            String metadataJson = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(metadata);
            fos.write(metadataJson.getBytes());
        }
    }

    public static void writeToBucket(String bucketName, Map<String, Object> fieldValues) throws IOException {
        GrugBucketMetadata metadata = BUCKET_METADATA_MAP.get(bucketName);
        DataOutputStream dos = BUCKET_METADATA_MAP.get(bucketName).getDos();

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

        dos.flush();
    }

    public static GrugReadResponse readFromBucket(String bucketName) throws IOException {
        GrugBucketMetadata metadata = BUCKET_METADATA_MAP.get(bucketName);
        List<GrugField> fields = metadata.getFields();

        int recordSize = 0;
        for (GrugField field : fields) {
            switch (field.getType()) {
                case INT:
                    recordSize += 4; // 4 bytes for int
                    break;
                case BOOLEAN:
                    recordSize += 1; // 1 byte for boolean
                    break;
                case FLOAT:
                    recordSize += 4; // 4 bytes for float
                    break;
                case STRING:
                    recordSize += field.getSize(); // Fixed size for string
                    break;
                default:
                    throw new IOException("Unsupported field type: " + field.getType());
            }
        }

        File bucketFile = new File(DIR, bucketName + ".grug");
        if (!bucketFile.exists() || bucketFile.length() == 0) {
            return null;
        }

        try (RandomAccessFile raf = new RandomAccessFile(bucketFile, "r")) {
            long fileLength = raf.length();
            if (fileLength < recordSize) {
                throw new IOException("File is too small to contain a complete record");
            }

            raf.seek(fileLength - recordSize);

            Map<String, Object> record = new HashMap<>();
            for (GrugField field : fields) {
                switch (field.getType()) {
                    case INT:
                        record.put(field.getName(), raf.readInt());
                        break;
                    case BOOLEAN:
                        record.put(field.getName(), raf.readBoolean());
                        break;
                    case FLOAT:
                        record.put(field.getName(), raf.readFloat());
                        break;
                    case STRING:
                        byte[] stringBytes = new byte[field.getSize()];
                        raf.readFully(stringBytes);
                        record.put(field.getName(), Utils.byteArrayToString(stringBytes));
                        break;
                    default:
                        throw new IOException("Unsupported field type: " + field.getType());
                }
            }
            return new GrugReadResponse(record);
        }
    }
}
