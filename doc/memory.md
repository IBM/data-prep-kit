### Memory and Endurance Considerations

A test was devised with a set of 1483 files on a Mac with 32GB memory and 4CPU cores. Traceback library was used to check for memory leak. 
10 iterations were run and the memory usage was observed, which peaked around 4 GB. There were no obvious signs of a memory leak. 

Another set of tests was done with the 1483 files on a podman VM with different memory configurations. The results are below.
It seems that it needed around 4GB of available memory to run successfully for all 1483 files.

|CPU Cores                     | Total Memory       | Memory Used by Ray | Transform           | Files Procesed Successfully       | Status of Job     | 
|------------------------------  |-------------------|------------------|--------------------|------------------------|
|4                          |8GB                     |          4.2GB   |NOOP   |1483 |Passed |
|4                          |6GB                     |          3GB     |NOOP   |910  |Crashed |
|4                          |4GB                     |          2GB     |NOOP   |504  |Crashed |

