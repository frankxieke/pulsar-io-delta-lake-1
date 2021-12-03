/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.deltalake.schema;

import io.delta.standalone.types.ArrayType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.MapType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SchemaConvert util used to convert avro schema and delta schema.
 */
public class SchemaConvert {
    private static final Logger log = LoggerFactory.getLogger(SchemaConvert.class);

    public static StructField convertOneAvroFieldToDeltaField(
            String name, Schema avroSchema) throws UnsupportedOperationException {
        StructField newField = null;
        switch(avroSchema.getType()) {
            case RECORD:
                StructType structType = new StructType();
                List<Schema.Field> allFields = avroSchema.getFields();
                StructField[] newFields = new StructField[allFields.size()];
                for (int i = 0; i < allFields.size(); i++) {
                    Schema.Field tmp = allFields.get(i);
                    newFields[i] = convertOneAvroFieldToDeltaField(tmp.name(), tmp.schema());
                }
                newField = new StructField(avroSchema.getName(), new StructType(newFields), true);
                break;
            case MAP:
                Schema valueSchema = avroSchema.getValueType();
                StructField deltaValueField = convertOneAvroFieldToDeltaField(valueSchema.getName(), valueSchema);
                log.info("value schema {} deltaSchema {}", valueSchema, deltaValueField);
                StructField keyField = new StructField("key", new StringType(), false);
                StructField valueField = new StructField("value", deltaValueField.getDataType(), false);
                StructField[] nfields = new StructField[2];
                nfields[0] = keyField;
                nfields[1] = valueField;
                StructType struct = new StructType(nfields);
                ArrayType arrayType = new ArrayType(struct, false);
                MapType mapType = new MapType(new StringType(), deltaValueField.getDataType(), false);
                newField = new StructField(name, mapType, true);
                break;
            case ARRAY:
                Schema itemSchema = avroSchema.getElementType();
                StructField deltaItemField = convertOneAvroFieldToDeltaField(itemSchema.getName(), itemSchema);
                arrayType = new ArrayType(deltaItemField.getDataType(), true);
                newField = new StructField(name, arrayType, true);
                break;
            case UNION:
                throw new UnsupportedOperationException("not support union in delta schema");
            case FIXED:
                throw new UnsupportedOperationException("not support fixed in delta schema");
            case STRING:
                newField = new StructField(name, new StringType(), true);
                break;
            case BYTES:
                newField = new StructField(name, new StringType(), avroSchema.isNullable());
                break;
            case INT:
                newField = new StructField(name, new IntegerType(), true);
                break;
            case LONG:
                newField = new StructField(name, new LongType(), avroSchema.isNullable());
                break;
            case FLOAT:
                newField = new StructField(name, new FloatType(), avroSchema.isNullable());
                break;
            case DOUBLE:
                newField = new StructField(name, new DoubleType(), avroSchema.isNullable());
                break;
            case BOOLEAN:
                newField = new StructField(name, new BooleanType(), avroSchema.isNullable());
                break;
            case NULL:
                newField = new StructField(name, new BooleanType(), avroSchema.isNullable());
                break;
            default:
                log.error("not support schema type {} in convert ", avroSchema.getType());
                break;
        }
        return newField;
    }
    public static StructType convertAvroSchemaToDeltaSchema(Schema pulsarAvroSchema) {
        log.info("pulsar schema {}", pulsarAvroSchema);
        Schema avroSchema = convertPulsarAvroSchemaToNonNullSchema(pulsarAvroSchema);
        System.out.println("after convert " + avroSchema);
        StructField field = convertOneAvroFieldToDeltaField(pulsarAvroSchema.getName(), avroSchema);
        return (StructType) field.getDataType();
    }

    public static Schema convertPulsarAvroSchemaToNonNullSchema(Schema schema) {
        List<org.apache.avro.Schema.Field> fields = schema.getFields();
        List<Schema.Field> newFields = new ArrayList<>();
        fields.forEach(f->{
            Schema fieldSchema = convertOneField(f.name(), f.schema());

                Schema.Field field = new org.apache.avro.Schema.Field(f.name(),
                        fieldSchema, fieldSchema.getDoc(), null);
                newFields.add(field);
        });

        org.apache.avro.Schema newSchema = org.apache.avro.Schema.createRecord(
                schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());

        newSchema.setFields(newFields);
        return newSchema;
    }

    public static Schema convertOneField(String name, org.apache.avro.Schema f) {
        org.apache.avro.Schema newField = null;
        switch(f.getType()) {
            case UNION:
                List<org.apache.avro.Schema> types = f.getTypes();
                List<org.apache.avro.Schema> validTypes = new ArrayList<>();
                types.forEach(t->{
                    if (t.getType() != org.apache.avro.Schema.Type.NULL) {
                        validTypes.add(t);
                    }
                });
                if (validTypes.size() == 1) {
                    newField = convertOneField(name, validTypes.get(0));
                } else {
                    log.error("not support this kind of union types {}", types);
                    throw new UnsupportedOperationException("not support this kind of field");
                }
                break;
            case RECORD:
                List<org.apache.avro.Schema.Field> fields = f.getFields();
                List<org.apache.avro.Schema.Field> newFields = new ArrayList<>();
                for (int i = 0; i < fields.size(); i++) {
                    org.apache.avro.Schema tmp = convertOneField(fields.get(i).name(), fields.get(i).schema());
                    newFields.add(new org.apache.avro.Schema.Field(fields.get(i).name(),
                            tmp, tmp.getDoc(), null));
                }
                newField = org.apache.avro.Schema.createRecord(
                        name, f.getDoc(), f.getNamespace(), f.isError());
                newField.setFields(newFields);
                break;
            case ARRAY:
                if (f.getElementType().getType() == Schema.Type.RECORD) {
                    // Map will be an array whose items is a RECORD with 'key' and 'value', but this kind of format
                    // is different from the pulsar json encoding
                    Schema.Field keyField = f.getElementType().getField("key");
                    Schema.Field valueField = f.getElementType().getField("value");
                    if (keyField != null && valueField != null) {
                        throw new UnsupportedOperationException("not support this kind of map datatype");
                    }
                }
                org.apache.avro.Schema newItem = convertOneField(f.getElementType().getName(), f.getElementType());
                newField = Schema.createArray(newItem);
                break;
            case MAP:
                newItem = convertOneField(name, f.getValueType());
                newField = Schema.createMap(newItem);
                break;
            default:
                newField = f;
        }
        return newField;
    }
}
