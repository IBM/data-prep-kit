# Define the root of the local git clone for the common rules to be able 
# know where they are running from.
REPOROOT=../../../../..
# Include a library of common .transform.* targets which most
# transforms should be able to reuse.  However, feel free
# to override/redefine the rules below. 

# $(REPOROOT)/.make.versions file contains the versions

DATA_PROFILER_RUNTIME=python
DATA_PROFILER_NAME=syntactic_construct_extractor

# include $(REPOROOT)/transforms/.make.transforms
include $(REPOROOT)/tools/data-profiler/.make.profiler

run-local-sample: .data-profiler.run-local-sample