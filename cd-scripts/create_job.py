import argparse
import json
import requests


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--databricks-instance", type=str, help="Databricks instance")
    parser.add_argument("--pat", type=str, help="Databricks personal access token")
    parser.add_argument("--wheel-version", type=str, help="Wheel version")
    parser.add_argument(
        "--dbfs-wheel-dir",
        type=str,
        default="dbfs:/wheels/example-ci-cd",
        help="New wheel version",
    )
    parser.add_argument("--scope", type=str, help="Databricks secret scope")
    parser.add_argument("--storage-uri", type=str, help="Databricks secret key for the Azure storage account URI")
    parser.add_argument("--storage-key", type=str, help="Databricks secret key for the Azure storage account key")

    args = parser.parse_args()

    create_job_url = f"https://{args.databricks_instance}/api/2.1/jobs/create"
    headers = {"Authorization": f"Bearer {args.pat}"}

    storage_account = "flomlworkshopprodstorage"
    container_name = "example-ci-cd"
    container_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net"

    job_conf = {
        "name": "job",
        "tasks": [
            {
                "task_key": "job",
                "description": "job",
                "existing_cluster_id": "1122-111352-rkzn7ywd",
                "python_wheel_task": {
                    "package_name": "example_ci_cd_databricks",
                    "entry_point": "job",
                    "named_parameters": {
                        "input-path": f"{container_path}/input_data",
                        "output-path": f"{container_path}/output_data",
                        "action": "init",
                        "scope": args.scope,
                        "storage-uri": args.storage_uri,
                        "storage-key": args.storage_key
                    }
                },
                "libraries": [
                    {
                        "whl": f"{args.dbfs_wheel_dir}/{args.wheel_version}"
                    }
                ]
            }
        ]
    }

    req = requests.post(create_job_url, data=json.dumps(job_conf), headers=headers)
    job_id = req.json()["job_id"]

    print(f"Job id: {job_id}")
