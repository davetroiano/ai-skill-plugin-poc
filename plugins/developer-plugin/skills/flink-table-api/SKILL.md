---
name: flink-table-api-skeleton
description: Creates sample or skeleton Flink Table API programs in Python or Java. Use this skill when users mention "flink table api", "flink java", "flink python", "table api", "confluent cloud flink", or want to create a basic streaming application with Flink. This skill generates complete skeleton code with Confluent Cloud connection setup, table definitions, filtering examples, configuration files, and comprehensive READMEs. Trigger even if the user just says "flink app" or "streaming app" in a Kafka/Confluent context.
---

# Flink Table API Skeleton Generator

This skill helps developers create skeleton Flink Table API applications for Confluent Cloud in either Python or Java.

## What This Skill Does

Generates a complete, runnable Flink Table API skeleton application that:
- Connects to Confluent Cloud
- Reads from an example Kafka topic/table
- Performs a simple filtering operation
- Includes all necessary configuration files
- Provides a comprehensive README with setup instructions

## Language Selection

If the user's prompt contains "python" or "py", generate a Python application.
If the user's prompt contains "java", generate a Java application.
If neither is specified, ask the user: "Would you like a Python or Java application? (I'll use Python if you don't specify)"
If the user doesn't respond with a clear choice, default to Python.

## Output Structure

### For Python Applications

Create the following structure:
```
flink-table-api-python/
├── filtering.py
├── cloud.properties
└── README.md
```

### For Java Applications

Create the following structure:
```
flink-table-api-java/
├── src/
│   └── main/
│       ├── java/
│       │   └── io/
│       │       └── confluent/
│       │           └── developer/
│       │               └── FlinkTableApiFiltering.java
│       └── resources/
│           └── cloud.properties
├── build.gradle
├── settings.gradle
└── README.md
```

## Python Application Template

### filtering.py

```python
from pyflink.table.confluent import ConfluentSettings, ConfluentTools
from pyflink.table import TableEnvironment
from pyflink.table.expressions import col

def filter_example():
    # Load Confluent Cloud settings from properties file
    settings = ConfluentSettings.from_file('./cloud.properties')
    env = TableEnvironment.create(settings)

    # Query the example table with filtering
    table_result = env.from_path('examples.marketplace.orders') \
        .select(col('customer_id'), col('product_id'), col('price')) \
        .filter(col('price') >= 50) \
        .execute()

    # Print first 5 materialized results
    ConfluentTools.print_materialized_limit(table_result, 5)

    # Alternative: iterate through results
    with table_result.collect() as rows:
        i = 0
        for row in rows:
            print(f"Price: {row[2]}")
            i += 1
            if i >= 5:
                break

if __name__ == '__main__':
    filter_example()
```

### Python README.md

Create a README with these sections:

1. **Title**: "How to filter Kafka messages in Python using Flink's Table API for Confluent Cloud"

2. **Prerequisites**:
   - A Confluent Cloud account (link: https://confluent.cloud/signup)
   - Confluent CLI installed (link: https://docs.confluent.io/confluent-cli/current/install.html)
   - Python 3.8 or later
   - Java 21 (required for Py4J, which Flink Python uses under the hood)

3. **Provision Confluent Cloud Infrastructure**:
   - Instructions to install the `confluent-flink-quickstart` plugin
   - Command to create resources and generate the properties file:
     ```bash
     confluent flink quickstart \
         --name flink_table_api_tutorials \
         --max-cfu 10 \
         --region us-east-1 \
         --cloud aws \
         --table-api-client-config-file ./cloud.properties
     ```

4. **Code Explanation**:
   - Explain how `ConfluentSettings.from_file()` loads the configuration
   - Explain how `TableEnvironment.create()` creates the environment
   - Explain the filtering operation on the example table
   - Explain both output methods (print_materialized_limit and collect)

5. **Run the Program**:
   ```bash
   python -m venv venv
   source ./venv/bin/activate
   pip install confluent-flink-table-api-python-plugin
   python filtering.py
   ```

6. **Expected Output**: Show example output with a table of results

7. **Tear Down**: Instructions to delete the environment, API key, and properties file

## Java Application Template

### FlinkTableApiFiltering.java

```java
package io.confluent.developer;

import io.confluent.flink.plugin.ConfluentSettings;
import io.confluent.flink.plugin.ConfluentTools;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkTableApiFiltering {

    public static void main(String[] args) throws Exception {
        // Load Confluent Cloud settings from properties file
        EnvironmentSettings envSettings = ConfluentSettings.fromResource("/cloud.properties");
        TableEnvironment tableEnv = TableEnvironment.create(envSettings);

        // Query the example table with filtering
        TableResult tableResult = tableEnv.from("examples.marketplace.orders")
                .select($("customer_id"), $("product_id"), $("price"))
                .filter($("price").isGreaterOrEqual(50))
                .execute();

        // Print first 5 materialized results
        ConfluentTools.printMaterialized(tableResult, 5);

        // Alternative: iterate through results
        try (CloseableIterator<Row> it = tableResult.collect()) {
            for (int i = 0; it.hasNext() && i < 5; i++) {
                Row row = it.next();
                System.out.println("Price: " + row.getField("price"));
            }
        }
    }
}
```

### build.gradle

```gradle
buildscript {
  repositories {
    mavenCentral()
  }
}

plugins {
  id 'java'
  id 'application'
}

java {
  sourceCompatibility = JavaVersion.VERSION_21
  targetCompatibility = JavaVersion.VERSION_21
}

application {
  mainClass = 'io.confluent.developer.FlinkTableApiFiltering'
}

repositories {
  mavenCentral()
  maven { url 'https://packages.confluent.io/maven/' }
}

dependencies {
  implementation 'org.apache.flink:flink-table-api-java:1.20.1'
  implementation 'io.confluent.flink:confluent-flink-table-api-java-plugin:1.20-50'
  implementation 'org.slf4j:slf4j-api:2.0.17'
  implementation 'org.slf4j:slf4j-simple:2.0.17'
}
```

### settings.gradle

```gradle
rootProject.name = 'flink-table-api-filtering'
```

### Java README.md

Create a README with these sections:

1. **Title**: "How to filter Kafka messages in Java using Flink's Table API for Confluent Cloud"

2. **Prerequisites**:
   - A Confluent Cloud account (link: https://confluent.cloud/signup)
   - Confluent CLI installed (link: https://docs.confluent.io/confluent-cli/current/install.html)
   - Java 21

3. **Provision Confluent Cloud Infrastructure**:
   - Instructions to install the `confluent-flink-quickstart` plugin
   - Command to create resources and generate the properties file:
     ```bash
     confluent flink quickstart \
         --name flink_table_api_tutorials \
         --max-cfu 10 \
         --region us-east-1 \
         --cloud aws \
         --table-api-client-config-file ./src/main/resources/cloud.properties
     ```

4. **Code Explanation**:
   - Explain the two required dependencies (flink-table-api-java and confluent-flink-table-api-java-plugin)
   - Explain how `ConfluentSettings.fromResource()` loads the configuration
   - Explain how `TableEnvironment.create()` creates the environment
   - Explain the filtering operation on the example table using the Table API DSL
   - Explain both output methods (printMaterialized and collect with CloseableIterator)

5. **Run the Program**:
   ```bash
   ./gradlew run
   ```

6. **Expected Output**: Show example output with a table of results

7. **Tear Down**: Instructions to delete the environment, API key, and properties file

## cloud.properties Template

For both Python and Java, create a `cloud.properties` file with this template:

```properties
client.cloud=
client.region=
client.flink-api-key=
client.flink-api-secret=
client.organization-id=
client.environment-id=
client.compute-pool-id=
client.principal-id=
```

Explain in the README that this file will be populated by the `confluent flink quickstart` command, or can be manually created following the documentation at https://docs.confluent.io/cloud/current/flink/reference/table-api.html#properties-file

## Implementation Guidelines

1. **Create all files**: Don't just outline what to create—actually create all the files in the appropriate directory structure.

2. **Use clear naming**: For Python, use `flink-table-api-python/` as the directory name. For Java, use `flink-table-api-java/`.

3. **Provide context**: In the README, explain why each piece exists. This is a learning skeleton, not just a copy-paste template.

4. **Keep it minimal**: Don't add extra features beyond the basic filtering example. The goal is a clear, understandable starting point.

5. **Make it runnable**: The code should work immediately after the user provisions Confluent Cloud resources and installs dependencies.

6. **Include troubleshooting hints**: In the README, mention that Java 21 is required and how to check the version.

## Example User Interactions

**User**: "create a basic python table api app"
→ Generate Python skeleton with filtering.py, cloud.properties, and README.md

**User**: "I need a flink java application for confluent cloud"
→ Generate Java skeleton with full Gradle project structure

**User**: "set up a flink table api project"
→ Ask: "Would you like a Python or Java application?"

**User**: "create flink streaming app"
→ Ask: "Would you like a Python or Java Flink Table API application?"

## After Generation

After creating all files, tell the user:
1. What files were created and where
2. The next steps to run the application (provision Confluent Cloud, install dependencies, run)
3. Remind them that the example uses the Confluent Cloud `examples.marketplace.orders` table, which is read-only and available by default
