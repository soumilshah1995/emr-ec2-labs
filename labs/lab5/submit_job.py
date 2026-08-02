import boto3
import time
from botocore.exceptions import ClientError

def submit_hive_step_to_emr(cluster_id, job_path, step_name):
    # Create an EMR client
    emr_client = boto3.client('emr', region_name='us-east-1')  # Change region as needed

    # Define the step configuration
    step_config = [
        {
            "Name": step_name,
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "hive",
                    "-f",
                    job_path
                ]
            }
        }
    ]

    try:
        # Add the step to the specified cluster
        response = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=step_config
        )

        # Get the ID of the newly added step
        step_id = response["StepIds"][0]
        print(f"Successfully submitted step with ID: {step_id}")

        return step_id  # Return the step ID for further processing

    except ClientError as e:
        print(f"Error submitting step: {e}")
        return None  # Return None if submission fails


def print_step_status(emr_client, cluster_id, step_id):
    while True:
        try:
            # Describe the step to get its status
            response = emr_client.describe_step(
                ClusterId=cluster_id,
                StepId=step_id
            )
            status = response['Step']['Status']['State']
            print(f"Current status of step {step_id}: {status}")

            # Check for different statuses
            if status == 'COMPLETED':
                print(f"Step {step_id} has completed successfully.")
                return True  # Return True if completed

            elif status in ['TERMINATED', 'CANCELLED','FAILED']:
                print(f"Step {step_id} has been {status}. Exiting polling.")
                return False  # Return False if terminated or canceled

            # If running, continue checking
            time.sleep(1)  # Wait for 1 second before polling again

        except ClientError as e:
            print(f"Error fetching step status: {e}")
            return False  # Return False on error


# Example usage
if __name__ == "__main__":
    cluster_id = "XXX"  # Replace with your actual cluster ID
    job_path = "s3://XXX/jobs/analytics.sql"  # Ensure there are no leading spaces
    step_name = "HiveJobDemo"

    # Submit the job and get the step ID
    step_id = submit_hive_step_to_emr(cluster_id, job_path, step_name)

    if step_id:
        # Create an EMR client for polling
        emr_client = boto3.client('emr', region_name='us-east-1')

        # Print the status of the submitted step every second and check for completion or errors
        success = print_step_status(emr_client, cluster_id, step_id)

        if success:
            print("The job completed successfully.")
        else:
            print("The job did not complete successfully or encountered an error.")
