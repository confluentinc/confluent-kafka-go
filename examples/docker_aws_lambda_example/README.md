# Build Docker image
    $ docker build -f examples/docker_aws_lambda_example/Dockerfile -t goclients .

# Push the docker image to AWS Elastic Container Registry
  Create Amazon Elastic Container Registry first
  Push the docker image to AWS ECR according to the [AWS ECR user guide](https://docs.aws.amazon.com/AmazonECR/latest/userguide/docker-push-ecr-image.html), or using all the commands under the `View push commands` of the ECR repository.

# Create AWS lambda function using image from AWS ECR
  Choose the `Container Image` when create the lambda function, add the docker image URL from `Container image URL`.

# Config Environment Variables
  Add the environment variables under the `Configuration`, we can pass the parameters like `bootstrap_servers`, `ccloudAPIKey`, `ccloudAPISecret` as environment variables. 

# Run the test
  Click the `Test` button under `Test`.
