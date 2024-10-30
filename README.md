# Open Brewery Database Pipeline

## Overview
This repository contains a data pipeline implemented in Databricks to extract, transform, and load (ETL) data from the Open Brewery API into a Delta Lake format. The pipeline consists of three layers: Bronze, Silver, and Gold, along with library and schema definitions to support the process.

## Orchestration Tool
Databricks was chosen as the primary tool for this project due to its powerful features for big data processing, ease of use, and integration with Delta Lake for efficient data storage and management. It allows for seamless scaling, collaborative work on notebooks, and provides a user-friendly interface.

## Project Structure
- `bronze_layer`: Contains the logic for extracting data from the Open Brewery API and saving it in the Bronze layer.
- `silver_layer`: Reads from the Bronze layer, transforms the data, and saves it in the Silver layer.
- `gold_layer`: Aggregates and summarizes data from the Silver layer into the Gold layer.
- `libs`: Contains library definitions and helper functions for data operations.
- `schemas`: Includes schema definitions for the Bronze and Silver layers.

## Step-by-Step Execution Guide

### 1. Set Up Databricks Workspace
   - Log in to your Databricks account.
   - Create a new workspace if you don't have one already.

### 2. Import the Necessary Files
   - Go to the **Workspace** section of Databricks.
   - Click on the option to **Import** notebooks.
   - Upload the following files from your local machine:
     - **bronze_layer**
     - **silver_layer**
     - **gold_layer**
     - **libs/tools**
     - **libs/schemas**
   - Ensure that all imported notebooks are placed in a structured folder (e.g., `/OpenBreweryDB`) for easy navigation.

### 3. Configure the Library Dependencies
   - Ensure that any required libraries (such as Delta Lake) are installed in your Databricks environment.
   - Go to the **Clusters** section, select your cluster, and under **Libraries**, install any necessary libraries.

### 4. Create a New Cluster
   - In the **Clusters** section, click on **Create Cluster**.
   - Choose an appropriate name for your cluster (e.g., `OpenBreweryDB-Cluster`).
   - Select the cloud provider (e.g., AWS, Azure, or GCP) and configure the cluster settings according to the job requirements. Use the specifications outlined in the provided JSON if necessary.
   - Click on **Create Cluster**.

### 5. Open the Notebooks
   - Navigate to each of the notebooks (`bronze_layer`, `silver_layer`, `gold_layer`, `libs`, `schemas`) within your imported folder.

### 6. Set Up Widgets for Parameters
   - In the **bronze_layer** notebook, ensure that you have set up the following widget to capture the `ROOT_PATH` parameter:
     ```python
     dbutils.widgets.text("ROOT_PATH", "file:/dbfs/test")
     ```
   - You can adjust the default value as needed for your environment.

### 7. Execute the Bronze Layer
   - Run the **bronze_layer** notebook. This notebook will:
     - Fetch data from the Open Brewery API.
     - Save the data into the Bronze layer in Delta format.

### 8. Execute the Silver Layer
   - After the bronze layer has completed successfully, run the **silver_layer** notebook. This notebook will:
     - Read the data from the Bronze layer.
     - Transform the data as necessary.
     - Save the transformed data into the Silver layer.

### 9. Execute the Gold Layer
   - Once the silver layer execution is complete, run the **gold_layer** notebook. This notebook will:
     - Read data from the Silver layer.
     - Perform further aggregations or transformations.
     - Save the final output into the Gold layer.

### 10. Configure Job Parameters (Optional)
   - If you want to automate the execution using a Databricks job, go to the **Jobs** section and create a new job.
   - Use the provided JSON configuration to set up the job.
   - Replace `<YOUR-DATABRICKS-WORKSPACE-PATH>` with the actual path to your notebooks.
   - Adjust any parameters such as `run_as` and `git_source` as necessary.
   - The retry policy is already on the job.json file with default of 1 retry

### 11. Run the Job (If Configured)
   - Once the job is set up, you can run it manually or configure it for scheduled execution.

### 12. Monitor the Pipeline Execution
   - Check the **Jobs** section for execution status and logs.
   - Ensure that each layer completes successfully and troubleshoot any errors if they arise.

### 13. Configuring Job Monitoring Notifications

To set up monitoring notifications for your Databricks job, follow these step-by-step instructions:

1. **Open Databricks Workspace**: Log in to your Databricks workspace where the job is created.

2. **Navigate to Jobs**: Click on the "Jobs" section from the left sidebar to view all your configured jobs.

3. **Select Your Job**: Find and select the job for which you want to set up notifications.

4. **Edit Job Settings**: Click on the “Edit” button to modify the job configuration.

5. **Scroll to Notifications**: In the job configuration page, scroll down to the “Email Notifications” section.

6. **Configure Email Notifications**: 
   - **Set Email Recipients**: Enter the email addresses of individuals who should receive notifications. You can specify multiple emails separated by commas.
   - **Choose Notification Triggers**: Select which events you want to be notified about, such as:
     - **No alert for skipped runs**: To receive alerts if runs are skipped.
     - **Job success**: To get notified when the job completes successfully.
     - **Job failure**: To receive alerts if the job fails.

7. **Save Changes**: After configuring the notifications, make sure to save your changes.

8. **Test Notifications**: Consider running the job to ensure that notifications are sent as expected and that recipients receive the execution logs.

By following these steps, you can ensure that your Databricks job is monitored effectively, allowing you to stay updated on its execution status and quickly address any issues that arise.


## Job Configuration
In this project, a job configuration is provided for running the pipeline. The job is set up to execute the `bronze_layer`, followed by the `silver_layer`, and finally the `gold_layer`. Each task will run upon the successful completion of the preceding one. The cluster is based in the AWS cloud, but the configuration can be edited to accommodate Azure and GCP as needed.


## Contribution

Contributions are welcome! Please open a pull request for improvements or fixes.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
