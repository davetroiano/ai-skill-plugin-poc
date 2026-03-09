# Streaming Skills Plugin Marketplace

A Claude AI plugin marketplace that provides specialized skills for streaming application developers. This marketplace includes plugins for working with Apache Kafka, Apache Flink, and Confluent Cloud services.

## What's Inside

This repository contains the **developer-plugin**, which adds AI skills for:

- **Apache Kafka Producers** - Generate production-ready Kafka producer code in multiple languages (Java, Python, Go, JavaScript, .NET) with Schema Registry integration
- **Apache Flink Table API** - Create skeleton Flink Table API applications for Confluent Cloud in Python or Java

The skills are designed to accelerate development by providing complete, runnable code skeletons with best practices, configuration files, and comprehensive documentation.

## Installation

### 1. Add the Marketplace

First, add this plugin marketplace to Claude:

```
/plugin marketplace add davetroiano/ai-skill-plugin-poc
```

### 2. Install the Plugin

Once the marketplace is added, install the developer plugin:

```
/plugin install developer-plugin@streaming-skills
```

### 3. Start Using the Skills

After installation, the skills will automatically activate based on your prompts. Here's an example:

```
make a flink table api program skeleton for me
```

This will trigger the Flink Table API skill, which will ask you whether you want a Python or Java application, then generate a complete skeleton project with:
- Sample filtering code
- Configuration templates
- Build files (if using Java)
- Comprehensive README with setup instructions

## Other Example Prompts

Try these prompts to activate different skills:

**For Kafka Producer:**
```
create a java kafka producer with schema registry
```

```
build me a python kafka producer with avro serialization
```

**For Flink Table API:**
```
create a basic python table api app
```

```
I need a flink java application for confluent cloud
```

## Skills Reference

### flink-table-api-skeleton

Generates complete Flink Table API skeleton applications that connect to Confluent Cloud, read from Kafka topics, and perform filtering operations.

**Languages**: Python, Java
**Includes**: Connection setup, table definitions, filtering examples, configuration files, and READMEs

### kafka-producer

Generates production-ready Kafka producer applications with Schema Registry integration.

**Languages**: Java, Python, Go, JavaScript, .NET
**Includes**: Producer configuration, serialization setup, error handling, and optimization profiles
