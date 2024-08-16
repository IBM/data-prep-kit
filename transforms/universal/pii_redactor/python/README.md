# PII Redactor Transform

This transform redacts Personally Identifiable Information (PII) from the input data.

The transform leverages the [Microsoft Presidio SDK](https://microsoft.github.io/presidio/) for PII detection and uses the Flair recognizer for entity recognition.

### Supported Entities

The transform detects the following PII entities by default:
- **PERSON**: Names of individuals
- **EMAIL_ADDRESS**: Email addresses
- **ORGANIZATION**: Names of organizations
- **DATE_TIME**: Dates and times
- **PHONE_NUMBER**: Phone number
- **CREDIT_CARD**: Credit card numbers

You can configure the entities to detect by passing the required entities as argument param ( **--pii_redactor_entities** ).
To know more about different entity types supported - [Entities](https://microsoft.github.io/presidio/supported_entities/)

### Redaction Techniques

Two redaction techniques are supported:
- **replace**: Replaces detected PII with a placeholder (default)
- **redact**: Removes the detected PII from the text

You can choose the redaction technique by passing it as an argument parameter (**--pii_redactor_operator**).

## Input and Output

### Input

The input data should be a `py.Table` with a column containing the text where PII detection and redaction will be applied. By default, this column is named `contents`.

**Example Input Table Structure:** Table 1: Sample input to the pii redactor transform

| contents            | doc_id |
|---------------------|--------|
| My name is John Doe | doc001 |
| I work at apple     | doc002 |


### Output

The output table will include the original columns plus an additional column `new_contents` which is configurable with redacted text and `detected_pii` 
column consisting the type of PII entities detected in that document for replace operator.

**Example Output Table Structure for replace operator:**

| contents            | doc_id | new_contents             | detected_pii     |
|---------------------|--------|--------------------------|------------------|
| My name is John Doe | doc001 | My name is `<PERSON>`    | `[PERSON]`       |
| I work at apple     | doc002 | I work at `<ORGANIZATION>` | `[ORGANIZATION]` |

When `redact` operator is chosen the output will look like below
 
**Example Output Table Structure for redact operator**

| contents            | doc_id | new_contents             | detected_pii     |
|---------------------|--------|--------------------------|------------------|
| My name is John Doe | doc001 | My name is  | `[PERSON]`       |
| I work at apple     | doc002 | I work at | `[ORGANIZATION]` |

## Running

### Launched Command Line Options 
The following command line arguments are available in addition to 
the options provided by 
the [python launcher](../../../../data-processing-lib/doc/python-launcher-options.md).

```
  --pii_redactor_entities PII_ENTITIES
                        list of PII entities to be captured for example: ["PERSON", "EMAIL"]
  --pii_redactor_operator REDACTOR_OPERATOR
                        Two redaction techniques are supported - replace(default), redact 
  --pii_redactor_transformed_contents PII_TRANSFORMED_CONTENT_COLUMN_NAME
                        Mention the column name in which transformed contents will be added. This is required argument. 
  --pii_redactor_score_threshold SCORE_THRESHOLD
                        The score_threshold is a parameter that sets the minimum confidence score required for an entity to be considered a match.
                        Provide a value above 0.6
```