#!/usr/bin/env bash
set -euo pipefail

ENV=${1:-dev}
REGION=${AWS_REGION:-us-east-1}
PROFILE=${AWS_PROFILE:-de_jay_east}
USE_PROFILE=${USE_PROFILE:-true}

STACK_PREFIX="dq-${ENV}"

# Decide whether to pass --profile based on USE_PROFILE
if [[ "$USE_PROFILE" == "true" ]]; then
  echo "Using AWS profile: $PROFILE"
  PROFILE_ARG=(--profile "$PROFILE")
else
  echo "Using AWS OIDC/Env credentials (no --profile)"
  PROFILE_ARG=()
fi

echo "Deploying Data Quest infra for env=${ENV}, region=${REGION}"

# 1. S3
aws cloudformation deploy \
  "${PROFILE_ARG[@]}" \
  --region "$REGION" \
  --stack-name "${STACK_PREFIX}-s3" \
  --template-file infra/01_s3.yml \
  --parameter-overrides Env="$ENV" \
  --capabilities CAPABILITY_NAMED_IAM

# 2. DynamoDB
aws cloudformation deploy \
  "${PROFILE_ARG[@]}" \
  --region "$REGION" \
  --stack-name "${STACK_PREFIX}-dynamodb" \
  --template-file infra/02_dynamodb.yml \
  --parameter-overrides Env="$ENV"

# 3. Secrets
aws cloudformation deploy \
  "${PROFILE_ARG[@]}" \
  --region "$REGION" \
  --stack-name "${STACK_PREFIX}-secrets" \
  --template-file infra/03_secrets.yml \
  --parameter-overrides Env="$ENV" ContactEmail="your_email@example.com" ContactPhone="+1-555-555-5555"

# 4. IAM
aws cloudformation deploy \
  "${PROFILE_ARG[@]}" \
  --region "$REGION" \
  --stack-name "${STACK_PREFIX}-iam" \
  --template-file infra/04_iam.yml \
  --parameter-overrides Env="$ENV" \
  --capabilities CAPABILITY_NAMED_IAM

# 5. Lambda (update CodeBucket / keys before running!)
aws cloudformation deploy \
  "${PROFILE_ARG[@]}" \
  --region "$REGION" \
  --stack-name "${STACK_PREFIX}-lambda" \
  --template-file infra/05_lambda.yml \
  --parameter-overrides Env="$ENV" CodeBucket="your-code-bucket" BlsLambdaKey="dq-bls-sync.zip" PopLambdaKey="dq-population-api.zip" \
  --capabilities CAPABILITY_NAMED_IAM

# 6. Glue job
aws cloudformation deploy \
  "${PROFILE_ARG[@]}" \
  --region "$REGION" \
  --stack-name "${STACK_PREFIX}-glue" \
  --template-file infra/06_glue.yml \
  --parameter-overrides Env="$ENV" GlueJobScriptLocation="s3://your-code-bucket/glue_job.py" \
  --capabilities CAPABILITY_NAMED_IAM

# 7. Step Functions
aws cloudformation deploy \
  "${PROFILE_ARG[@]}" \
  --region "$REGION" \
  --stack-name "${STACK_PREFIX}-stepfunctions" \
  --template-file infra/07_stepfunctions.yml \
  --parameter-overrides Env="$ENV" \
  --capabilities CAPABILITY_NAMED_IAM

# 8. EventBridge
aws cloudformation deploy \
  "${PROFILE_ARG[@]}" \
  --region "$REGION" \
  --stack-name "${STACK_PREFIX}-eventbridge" \
  --template-file infra/08_eventbridge.yml \
  --parameter-overrides Env="$ENV" \
  --capabilities CAPABILITY_NAMED_IAM

echo "All stacks deployed."
