docker build -t prefect-dbt-k8s-snowflake .

aws ecr create-repository --repository-name prefect-dbt-k8s-snowflake --image-scanning-configuration scanOnPush=true
aws ecr get-login-password | docker login --username AWS --password-stdin XXXX.dkr.ecr.us-east-1.amazonaws.com
docker tag prefect-dbt-k8s-snowflake:latest XXXX.dkr.ecr.us-east-1.amazonaws.com/prefect-dbt-k8s-snowflake:latest
docker push XXXX.dkr.ecr.us-east-1.amazonaws.com/prefect-dbt-k8s-snowflake:latest
prefect register --project jaffle_shop -p flows/

prefect agent kubernetes install -k YOUR_API_KEY --rbac --label prod
kubectl apply -f k8s_agent.yaml
