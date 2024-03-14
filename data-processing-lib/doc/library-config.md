# Library Configuration
The library has a number of configurable values that can help provide or
override default values.  All are set using environment variables
in [DPFConfig](../src/data_processing/utils/config.py).

| Environment Variable  | Function                                                                                                         |
|-----------------------|------------------------------------------------------------------------------------------------------------------|
| **DPF_S3_ACCESS_KEY** | Sets the default S3 access key for S3 credential command line options                                            |
| AWS_ACCESS_KEY_ID     | Used if DPF_S3_ACCESS_KEY is not set.                                                                            |
| COS_ACCESS_KEY        | Used if DPF_S3_ACCESS_KEY  and AWS_ACCESS_KEY_ID are not set                                                     |
|                       |                                                                                                                  |
| **DPF_S3_SECRET_KEY**     | Sets the default S3 secret key for the S3 credentials command line config                                        |
| AWS_SECRET_ACCESS_KEY | Used if DPF_S3_SECRET_KEY is not set.                                                                            |
| COS_SECRET_KEY        | Used if DPF_S3_SECRET_KEY  and AWS_SECRET_ACCESS_KEY are not set                                                 |
|                       |                                                                                                                  |
| **DPF_LOG_LEVEL**         | Sets the log level to one of INFO, WARN, ERROR or DEBUG.  The latter will provide line number links for an IDE. |
