# Build Docker image
    $ docker buildx create --use
    $ docker buildx build \
    --push \
    --platform linux/arm/v7,linux/arm64/v8,linux/amd64 \ --tag your-username/multiarch-example:buildx-latest .

# Push the docker image to AWS Elastic Container Registry
Create Amazon Elastic Container Registry first
Push the docker image to AWS ECR according to the [AWS ECR user guide](https://docs.aws.amazon.com/AmazonECR/latest/userguide/docker-push-ecr-image.html), or using all the commands under the `View push commands` of the ECR repository.

Example:

    $ export REPO=cc-base
    $ export REGISTRY_BASE=755363985185.dkr.ecr.us-west-2.amazonaws.com/docker/dev/$REPO
    $ export SOURCE_PRODUCER_IMAGE=kafka-producer-spire-chang
    $ export PRODUCER_IMAGE=kafka-producer-spire-chang

# Use [Kompose](https://kompose.io/) to convert Docker Compose file to Kubernetes

    curl -L https://github.com/kubernetes/kompose/releases/download/v1.26.0/kompose-darwin-amd64 -o kompose 
    chmod +x kompose
    sudo mv ./kompose /usr/local/bin/kompose
    kompose convert

Run `kompose convert` in the same directory as the docker-compose.yaml file. 

# Then use kubectl apply to create these resources in Kubernetes:
    kubectl apply -f producer-deployment.yaml