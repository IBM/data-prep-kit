# PII Redactor Transform

This transform redacts Personally Identifiable Information (PII) from the input data.

The transform leverages the [Microsoft Presidio SDK](https://microsoft.github.io/presidio/) for PII detection and uses the Flair recognizer for entity recognition.

### Supported Entities

The transform detects the following PII entities by default:
- **PERSON**: Names of individuals
- **EMAIL_ADDRESS**: Email addresses
- **ORGANIZATION**: Names of organizations
- **DATE_TIME**: Dates and times

You can configure the entities to detect by passing the required entities as argument param ( **--pii_redactor_entities** ).
To know more about different entity types supported - [Entities](https://microsoft.github.io/presidio/supported_entities/)

### Redaction Techniques

Two redaction techniques are supported:
- **replace**: Replaces detected PII with a placeholder (default)
- **redact**: Removes the detected PII from the text

You can choose the redaction technique by passing it as an argument parameter (**--pii_redactor_operator**).

## Input and Output

### Input

The input data should be a `py.Table` with a column containing the text where PII detection and redaction will be applied. By default, this column is named `contents`, but this can be configured.

**Example Input Table Structure:**

| contents               | doc_id |
|------------------------|--------|
| My name is JohnDoe    | doc001 |
| I work at apple        | doc002 |

### Output

The output table will include the original columns plus an additional column `new_contents` with redacted text.

**Example Output Table Structure:**

| contents               | doc_id | new_contents           |
|------------------------|--------|------------------------|
| My name is JohnDoe    | doc001 | My name is <NAME>     |
| I work at apple        | doc002 | I work at <ORGANIZATION> |

## Running

### Launched Command Line Options 
The following command line arguments are available in addition to 
the options provided by 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).

```
  --pii_redactor_entities PII_ENTITIES
                        list of PII entities to be captured for example: ["PERSON", "EMAIL"]
  --pii_redactor_operator ANONYMIZATION_OPERATOR
                        Two redaction techniques are supported - replace(default), redact 
  --pii_redactor_contents PII_CONTENT_COLUMN_NAME
                        mention the column name for which pii redation transform has to be applied

```