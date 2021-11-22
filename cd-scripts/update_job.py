import argparse
import json
import requests


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--workspace-url", type=str, help="Databricks workspace url, starting with 'https'")
    parser.add_argument("--pat", type=str, help="Databricks personal access token")
    parser.add_argument("--wheel-version", type=str, help="New wheel version")
    parser.add_argument(
        "--dbfs-wheel-dir",
        type=str,
        default="dbfs:/wheels/example-ci-cd",
        help="New wheel version",
    )
    parser.add_argument("--job-id", type=str, help="Job id")

    args = parser.parse_args()

    update_job_url = f"{args.workspace_url}/api/2.1/jobs/update"
    headers = {"Authorization": f"Bearer {args.pat}"}

    cleaned_wheel_version = args.wheel_version.lstrip("v")
    full_wheel_name = f"example_ci_cd_databricks-{cleaned_wheel_version}-py3-none-any.whl"

    job_conf = {
        "job_id": args.job_id,
        "new_settings":
            {
                "libraries": [
                    {
                        "whl": f"{args.dbfs_wheel_dir}/{full_wheel_name}"
                    }
                ]
            }
    }

    req = requests.post(update_job_url, data=json.dumps(job_conf), headers=headers)
    print(req.text)

