# This script will grant required permissions to access data and metadata for end-users per group

# input format:
# prepare_end_user_permissions.sh group@domain.com host-project "project1 project2 project3 etc"
# This will grant group@domain.com permissions on the list of projects 1,2,3
# and on host-project

set -e

n=1
group=${!n}

n=2
host_project=${!n}

n=3
data_projects=${!n}

echo "Group is ${group}"
echo "Host project is [${host_project}]"
echo "Projects are [${data_projects}]"


##### Data Catalog Viewer on the host project (to view policy tags in BigQuery UI)
gcloud projects add-iam-policy-binding "${host_project}" \
  --member="group:${group}" \
  --role="roles/datacatalog.viewer"

##### For each project with marketing datasets
for project in $data_projects; do
  echo "Preparing permissions for group '${group}' on project '${project}' .."

  ##### BigQuery Reader (to read data)
  gcloud projects add-iam-policy-binding "${project}" \
    --member="group:${group}" \
    --role="roles/bigquery.dataViewer"

    ##### BigQuery Job User (to submit query jobs)
  gcloud projects add-iam-policy-binding "${project}" \
    --member="group:${group}" \
    --role="roles/bigquery.jobUser"


done