# Schema Registry and Schema Evolution Guide

Complete guide for working with Confluent Schema Registry, Avro schemas, schema evolution, and compatibility modes.

## Schema Registry Overview

Schema Registry is a centralized repository for managing and validating schemas for topic message data. It provides:

- **Schema storage**: Central repository for Avro, Protobuf, and JSON schemas
- **Schema versioning**: Track schema changes over time
- **Compatibility checking**: Enforce evolution rules to prevent breaking changes
- **Serialization efficiency**: Schemas stored once, messages contain only schema ID

### Why Use Schema Registry?

1. **Data quality**: Enforces structure on messages, preventing malformed data
2. **Evolution**: Enables safe schema changes without breaking consumers
3. **Documentation**: Schemas serve as contracts between producers and consumers
4. **Efficiency**: Binary serialization (Avro) is more compact than JSON
5. **Type safety**: Compile-time validation in strongly-typed languages

---

## Avro Schemas

### Basic Avro Schema Structure

```json
{
  "type": "record",
  "name": "RecordName",
  "namespace": "com.example.namespace",
  "doc": "Description of this record",
  "fields": [
    {
      "name": "fieldName",
      "type": "string",
      "doc": "Field description"
    }
  ]
}
```

### Avro Primitive Types

```json
{
  "fields": [
    {"name": "stringField", "type": "string"},
    {"name": "intField", "type": "int"},
    {"name": "longField", "type": "long"},
    {"name": "floatField", "type": "float"},
    {"name": "doubleField", "type": "double"},
    {"name": "booleanField", "type": "boolean"},
    {"name": "bytesField", "type": "bytes"},
    {"name": "nullField", "type": "null"}
  ]
}
```

### Complex Types

#### Optional Fields (Union with null)

```json
{
  "name": "optionalEmail",
  "type": ["null", "string"],
  "default": null
}
```

**Important**: In unions with null, null must come first for default value to work.

#### Arrays

```json
{
  "name": "tags",
  "type": {
    "type": "array",
    "items": "string"
  },
  "default": []
}
```

#### Maps

```json
{
  "name": "metadata",
  "type": {
    "type": "map",
    "values": "string"
  },
  "default": {}
}
```

#### Enums

```json
{
  "name": "status",
  "type": {
    "type": "enum",
    "name": "UserStatus",
    "symbols": ["ACTIVE", "INACTIVE", "SUSPENDED"]
  }
}
```

#### Nested Records

```json
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {
      "name": "address",
      "type": {
        "type": "record",
        "name": "Address",
        "fields": [
          {"name": "street", "type": "string"},
          {"name": "city", "type": "string"},
          {"name": "zipCode", "type": "string"}
        ]
      }
    }
  ]
}
```

### Logical Types

Logical types provide semantic meaning to primitive types:

#### Timestamps

```json
{
  "name": "createdAt",
  "type": "long",
  "logicalType": "timestamp-millis"
}
```

```json
{
  "name": "updatedAt",
  "type": "long",
  "logicalType": "timestamp-micros"
}
```

#### Decimals

```json
{
  "name": "price",
  "type": "bytes",
  "logicalType": "decimal",
  "precision": 10,
  "scale": 2
}
```

#### UUID

```json
{
  "name": "userId",
  "type": "string",
  "logicalType": "uuid"
}
```

#### Date and Time

```json
{
  "name": "birthDate",
  "type": "int",
  "logicalType": "date"
}
```

```json
{
  "name": "appointmentTime",
  "type": "int",
  "logicalType": "time-millis"
}
```

---

## Schema Evolution and Compatibility

### Compatibility Modes

Schema Registry supports different compatibility modes that determine what schema changes are allowed:

#### BACKWARD (Default)

- **New schema can read data written with old schema**
- **Use case**: When consumers update before producers
- **Allowed changes**:
  - Delete fields (if they had defaults)
  - Add optional fields (with defaults)
- **Forbidden changes**:
  - Add required fields
  - Delete fields without defaults
  - Change field types

```json
// Version 1
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "age", "type": "int", "default": 0}
  ]
}

// Version 2 (BACKWARD compatible)
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"},
    // age field deleted (had default) ✓
    {"name": "phone", "type": ["null", "string"], "default": null}  // optional field added ✓
  ]
}
```

#### FORWARD

- **Old schema can read data written with new schema**
- **Use case**: When producers update before consumers
- **Allowed changes**:
  - Add fields (with or without defaults)
  - Delete optional fields
- **Forbidden changes**:
  - Delete required fields
  - Change field types

```json
// Version 1
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"}
  ]
}

// Version 2 (FORWARD compatible)
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "phone", "type": "string"}  // New field added ✓
  ]
}
```

Old consumers will simply ignore the new `phone` field.

#### FULL

- **Combination of BACKWARD and FORWARD**
- **Both old and new schemas can read each other's data**
- **Use case**: Maximum flexibility for rolling updates
- **Allowed changes**:
  - Add optional fields (with defaults)
  - Delete optional fields (with defaults)
- **Forbidden changes**:
  - Add required fields
  - Delete required fields
  - Change field types

```json
// Version 1
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "age", "type": ["null", "int"], "default": null}
  ]
}

// Version 2 (FULL compatible)
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"},
    // age deleted (was optional with default) ✓
    {"name": "phone", "type": ["null", "string"], "default": null}  // optional field added ✓
  ]
}
```

#### NONE

- **No compatibility checking**
- **Use case**: Development, prototyping
- **Warning**: Can break consumers without warning

#### Transitive Variants

- **BACKWARD_TRANSITIVE**: New schema compatible with all previous versions
- **FORWARD_TRANSITIVE**: All previous schemas compatible with new schema
- **FULL_TRANSITIVE**: Bidirectional compatibility across all versions

---

## Schema Evolution Best Practices

### 1. Always Provide Defaults for New Fields

```json
// Good: New optional field with default
{
  "name": "phoneNumber",
  "type": ["null", "string"],
  "default": null
}

// Bad: New required field (breaks BACKWARD compatibility)
{
  "name": "phoneNumber",
  "type": "string"
}
```

### 2. Never Change Field Types

```json
// Bad: Changing type breaks compatibility
// Version 1
{"name": "age", "type": "int"}

// Version 2 - INCOMPATIBLE
{"name": "age", "type": "string"}
```

If you must change types, add a new field and deprecate the old one:

```json
// Better approach
{
  "fields": [
    {"name": "age", "type": "int"},  // Deprecated but kept for compatibility
    {"name": "ageString", "type": "string"}  // New field
  ]
}
```

### 3. Be Careful with Enums

Adding enum symbols is FORWARD compatible but not BACKWARD compatible:

```json
// Version 1
{
  "name": "status",
  "type": {
    "type": "enum",
    "name": "Status",
    "symbols": ["ACTIVE", "INACTIVE"]
  }
}

// Version 2 - FORWARD compatible only
{
  "name": "status",
  "type": {
    "type": "enum",
    "name": "Status",
    "symbols": ["ACTIVE", "INACTIVE", "SUSPENDED"]  // New symbol
  }
}
```

Old consumers won't recognize "SUSPENDED". Consider using strings instead for maximum flexibility.

### 4. Plan for Deletion

If you might delete a field later, make it optional from the start:

```json
{
  "name": "temporaryField",
  "type": ["null", "string"],
  "default": null
}
```

### 5. Use Descriptive Field Names

```json
// Good
{"name": "userCreatedAtTimestampMillis", "type": "long"}

// Less clear
{"name": "timestamp", "type": "long"}
```

### 6. Document Breaking Changes

```json
{
  "type": "record",
  "name": "User",
  "doc": "v2.0: Removed 'age' field, added 'dateOfBirth' instead",
  "fields": [...]
}
```

---

## Working with Schema Registry

### Registering a Schema (REST API)

```bash
curl -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @schema.json \
  "https://psrc-xxxxx.region.provider.confluent.cloud/subjects/users-value/versions" \
  -u "${SCHEMA_REGISTRY_API_KEY}:${SCHEMA_REGISTRY_API_SECRET}"
```

### Checking Compatibility

```bash
curl -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @new-schema.json \
  "https://psrc-xxxxx.region.provider.confluent.cloud/compatibility/subjects/users-value/versions/latest" \
  -u "${SCHEMA_REGISTRY_API_KEY}:${SCHEMA_REGISTRY_API_SECRET}"
```

Response:
```json
{
  "is_compatible": true
}
```

### Setting Compatibility Mode

```bash
# For a specific subject
curl -X PUT \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "FULL"}' \
  "https://psrc-xxxxx.region.provider.confluent.cloud/config/users-value" \
  -u "${SCHEMA_REGISTRY_API_KEY}:${SCHEMA_REGISTRY_API_SECRET}"

# Globally
curl -X PUT \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "BACKWARD"}' \
  "https://psrc-xxxxx.region.provider.confluent.cloud/config" \
  -u "${SCHEMA_REGISTRY_API_KEY}:${SCHEMA_REGISTRY_API_SECRET}"
```

### Listing Schemas

```bash
# List all subjects
curl "https://psrc-xxxxx.region.provider.confluent.cloud/subjects" \
  -u "${SCHEMA_REGISTRY_API_KEY}:${SCHEMA_REGISTRY_API_SECRET}"

# List versions for a subject
curl "https://psrc-xxxxx.region.provider.confluent.cloud/subjects/users-value/versions" \
  -u "${SCHEMA_REGISTRY_API_KEY}:${SCHEMA_REGISTRY_API_SECRET}"

# Get specific version
curl "https://psrc-xxxxx.region.provider.confluent.cloud/subjects/users-value/versions/1" \
  -u "${SCHEMA_REGISTRY_API_KEY}:${SCHEMA_REGISTRY_API_SECRET}"
```

---

## Schema Naming Conventions

### Subject Naming Strategy

Three strategies for subject names:

1. **TopicNameStrategy** (default): `<topic-name>-value` or `<topic-name>-key`
   - Example: `users-value`, `users-key`
   - Use when: One schema per topic

2. **RecordNameStrategy**: `<namespace>.<record-name>`
   - Example: `com.example.User`
   - Use when: Multiple record types in same topic

3. **TopicRecordNameStrategy**: `<topic-name>-<namespace>.<record-name>`
   - Example: `events-com.example.UserCreated`
   - Use when: Multiple record types per topic, want topic-level organization

Configure in producer:

```properties
# Python
value.subject.name.strategy=io.confluent.kafka.serializers.subject.RecordNameStrategy

# Java
value.subject.name.strategy=io.confluent.kafka.serializers.subject.RecordNameStrategy
```

---

## Migration Strategies

### Migrating to a New Schema

#### Strategy 1: Dual Write (Safest)

1. Add new optional field to schema
2. Update producers to write both old and new fields
3. Deploy updated consumers that read new field
4. Update producers to stop writing old field
5. Remove old field from schema (if using BACKWARD)

#### Strategy 2: Shadow Topic

1. Create new topic with new schema
2. Update producers to write to both topics
3. Update consumers to read from new topic
4. Decommission old topic

#### Strategy 3: Schema Registry Aliasing

Use schema aliases to reference fields with different names:

```json
{
  "name": "emailAddress",
  "type": "string",
  "aliases": ["email"]  // Old field name
}
```

---

## Common Schema Patterns

### Event Envelope Pattern

```json
{
  "type": "record",
  "name": "EventEnvelope",
  "namespace": "com.example.events",
  "fields": [
    {"name": "eventId", "type": "string", "logicalType": "uuid"},
    {"name": "eventType", "type": "string"},
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "version", "type": "string"},
    {
      "name": "payload",
      "type": [
        "com.example.events.UserCreated",
        "com.example.events.UserUpdated",
        "com.example.events.UserDeleted"
      ]
    },
    {
      "name": "metadata",
      "type": {
        "type": "map",
        "values": "string"
      },
      "default": {}
    }
  ]
}
```

### Versioned Payload Pattern

```json
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "schemaVersion", "type": "string", "default": "2.0"},
    {"name": "id", "type": "string"},
    // ... rest of fields
  ]
}
```

### Polymorphic Records (Union)

```json
{
  "name": "event",
  "type": [
    {
      "type": "record",
      "name": "UserCreated",
      "fields": [...]
    },
    {
      "type": "record",
      "name": "UserUpdated",
      "fields": [...]
    }
  ]
}
```

---

## Troubleshooting

### Error: "Schema being registered is incompatible"

**Cause**: New schema violates compatibility mode rules

**Solutions**:
1. Check compatibility mode: `curl .../config/subject-name`
2. Test compatibility before registering
3. Adjust schema to be compatible
4. Consider changing compatibility mode (carefully!)

### Error: "Subject not found"

**Cause**: Schema hasn't been registered yet

**Solution**: Register schema first, or enable auto-registration in producer config:
```properties
auto.register.schemas=true
```

### Error: "Schema already exists"

**Cause**: Trying to register identical schema

**Solution**: This is actually fine - Schema Registry will return existing schema ID

### Warning: Schemas Growing Large

**Solution**: Split into smaller, focused schemas using references:

```json
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {
      "name": "address",
      "type": "com.example.Address"  // Reference to another schema
    }
  ]
}
```

---

## Best Practices Summary

1. ✅ **Start with BACKWARD or FULL compatibility** for production
2. ✅ **Always provide defaults** for optional fields
3. ✅ **Never change field types** - add new fields instead
4. ✅ **Test compatibility** before deploying schema changes
5. ✅ **Use logical types** for timestamps, decimals, UUIDs
6. ✅ **Document schemas** with `doc` fields
7. ✅ **Version your schemas** semantically in doc/metadata
8. ✅ **Plan for evolution** from the start
9. ❌ **Don't delete required fields** without a migration plan
10. ❌ **Don't change enum symbols** without careful planning
