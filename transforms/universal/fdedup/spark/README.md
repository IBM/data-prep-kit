# Spark-GUF

This is an implementation of Spark data processing modules. At a high level, every Spark application consists of a driver program that runs the user’s main function and executes various parallel operations on a cluster.  

The modules can run locally or remotely in a Kubernetes cluster.

## Running Transforms locally

Start in the `spark-guf` directory. To run the modules locally, follow these steps:
1. Create a virtual environment using this command
   ```
   make venv
   ```
2. Activate the virtual environment:
   ```
   source venv/bin/activate
   ```

3. Set the `PYTHONPATH` environment variable to include the `src` directory:
   ```
   export PYTHONPATH=${PYTHONPATH}:${PWD}/src
   ```
4. Invoke one of the transforms:
   ```
   python src/transforms/spark_pi/spark_transformer_pi.py
   ```
5. To find out which arguments a transform takes, run that transform with a `--help` flag:
   ```
   python src/transforms/spark_filter/spark_filter_transform.py --help
   usage: spark_filter_transform.py [-h] --input_folder INPUT_FOLDER --output_folder OUTPUT_FOLDER [--data_type DATA_TYPE]
                                    --filter_criteria_list FILTER_CRITERIA_LIST [--filter_columns_to_drop FILTER_COLUMNS_TO_DROP]
                                    [--filter_logical_operator {AND,OR}]

   optional arguments:
      -h, --help            show this help message and exit
      --input_folder INPUT_FOLDER
                            path to read the input files (local fs or s3)
      --output_folder OUTPUT_FOLDER
                            path to write the output files (local fs or s3)
      --data_type DATA_TYPE
                            Type of files to filter (parquet, orc, csv, json, txt)
      --filter_criteria_list FILTER_CRITERIA_LIST
                            list of filter criteria (in SQL WHERE clause format), for example: [ "docq_total_words > 100 AND docq_total_words < 200", "docq_perplex_score < 230", "date_acquired BETWEEN '2023-07-04'
                            AND '2023-07-08'", "title LIKE 'https://%'", "document_id IN ('doc-id-1', 'doc-id-2', 'doc-id-3')" ]
      --filter_columns_to_drop FILTER_COLUMNS_TO_DROP
                            list of columns to drop after filtering, for example: ["column1", "column2"]
      --filter_logical_operator {AND,OR}
                            logical operator (AND or OR) that joins filter criteria
   ```

## Running Transforms in Kubernetes/OpenShift

Start in the `spark-guf` directory. To run the transforms in a Kubernetes or OpenShift cluster, follow these steps:

1. Build and push a pyspark base docker image (this example assumes that images are pushed to the Docker hub, but same approach can be used to push images to icr.io, or quai.io:
   ```
   docker build -t my-docker-username/my-pyspark:3.5.1 .
   docker push my-docker-username/my-pyspark:3.5.1
   ```  
2. Build and push a specific transform image (this will use the pyspark built in the previous point as the base image):
   ```
   docker build -t my-docker-username/my-pyspark-filter:3.5.1 -f src/transforms/spark_filter/Dockerfile --build-arg BASE_IMAGE=my-docker-username/my-pyspark:3.5.1 .
   docker push my-docker-username/my-pyspark-filter:3.5.1 
   ```

3. Configure the `spark` service account (note that you can use any other service account name, but you will need then to replace `spark` with `your-service-account-name` in all the yaml files listed below). This is a one-time process to perform for each namespace where you want to run spark apps:
   ```
   # create 'spark' service account
   kubectl apply -f deployment/kubernetes/spark_sa_rb/spark-serviceaccount.yaml --namespace=my-namespace

   # create 'spark' role
   kubectl apply -f deployment/kubernetes/spark_sa_rb/spark-role.yaml --namespace=my-namespace

   # bind the 'spark' service account to the 'spark' role
   kubectl apply -f deployment/kubernetes/spark_sa_rb/spark-role-binding.yaml --namespace=my-namespace

   # bind the 'spark' service account to the cluster roles
   kubectl apply -f deployment/kubernetes/spark_sa_rb/spark-edit-role-binding.yaml --namespace=my-namespace
   kubectl apply -f deployment/kubernetes/spark_sa_rb/spark-cluster-role-binding.yaml --namespace=my-namespace
   ```
   
 4. Create any secrets that are needed to access S3 folders used for input or output of the transforms. Follow [this link](https://github.com/aws-samples/machine-learning-using-k8s/blob/master/docs/aws-creds-secret.md) for more information on how to build the S3 secrets.
 
 5. Edit a pod yaml file from the `deployment/kubernetes/pods` directory.  The steps below refer to the [yaml file used to build the filter pod] (deployment/kubernetes/pods/spark-driver-pod-filter.yaml):
    1. Give a name to the pod (`metadata/name`), the container launched inside the pod (`spec/containers/name`), and the Spark application (the `APP_NAME` variable in `spec/containers/env`).
    2. Specify the namespace where the pod will be created (`metadata/namespace`). Use the same namespace for the `EXECUTOR_NAMESPACE` variable in `spec/containers/env`)
    3. Specify the command to launch the Spark application (in `spec/containers/args`)
    4. Specify the image used by the driver (`spec/containers/image` - usually this is the transform image built under point 2).
    5. Specify the image used by the executors (`EXECUTOR_DOCKER_IMAGE` variable in `spec/containers/env`)
    6. Specify the service account to use by the driver (`spec/containers/serviceAccount`) and by the executors(the `SERVICE_ACCOUNT` variable in `spec/containers/env`)
    7. Configure S3: 
       1. Specify the input (`AWS_ENDPOINT_URL_IN`) and output (`AWS_ENDPOINT_URL_OUT`) endpoint URLs.  
       2. Specify the input and out access key ids and secret access keys.

6. Launch the Spark application by creating the driver pod:
   ```
   kubectl apply -f deployment/kubernetes/pod/spark-driver-pod-filter.yaml
   ```
   
7. Monitor the creation of the executor pods:
   ```
   kubectl get pods -w
   ```

8. Monitor the driver logs:
   ```
   kubectl logs spark-driver-pod-filter -f
   ```
   ```
