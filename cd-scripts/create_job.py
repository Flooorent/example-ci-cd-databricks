import argparse
import json
import requests


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--workspace-url", type=str, help="Databricks workspace url, starting with 'https'")
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
    parser.add_argument(
        "--storage-account",
        type=str,
        default="flomlworkshopprodstorage",
        help="Azure storage account where we read and write data",
    )
    parser.add_argument(
        "--container",
        type=str,
        default="example-ci-cd",
        help="Azure storage account's container name where we read and write data",
    )
    # NB: for demo purpose we use an existing cluster, but with real production jobs it's better to use a jobs-compute
    # cluster instead of an all-purpose cluster as it's simply way less expensive
    parser.add_argument(
        "--cluster-id",
        type=str,
        default="1122-111352-rkzn7ywd",
        help="Databricks cluster id to use when running the job",
    )
    parser.add_argument(
        "--package-name",
        type=str,
        default="example_ci_cd_databricks",
        help="Python project's package name",
    )

    args = parser.parse_args()

    create_job_url = f"{args.workspace_url}/api/2.1/jobs/create"
    headers = {"Authorization": f"Bearer {args.pat}"}

    container_path = f"abfss://{args.container}@{args.storage_account}.dfs.core.windows.net"

    cleaned_wheel_version = args.wheel_version.lstrip("v")
    full_wheel_name = f"{args.package_name}-{cleaned_wheel_version}-py3-none-any.whl"

    job_conf = {
        "name": "job",
        "tasks": [
            {
                "task_key": "job",
                "description": "job",
                "existing_cluster_id": args.cluster_id,
                "python_wheel_task": {
                    "package_name": args.package_name,
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
                        "whl": f"{args.dbfs_wheel_dir}/{full_wheel_name}"
                    }
                ]
            }
        ]
    }

    req = requests.post(create_job_url, data=json.dumps(job_conf), headers=headers)
    job_id = req.json()["job_id"]

    print(f"Job id: {job_id}")
