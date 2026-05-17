# One-time setup: GitHub Actions OIDC + deploy role for stand-backend-dev
# Requires: AWS CLI, credentials with IAM/CloudFormation permissions (us-east-1)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Template = Join-Path $Root "infra\github-actions-oidc.yaml"

Write-Host "Deploying OIDC stack (stand-github-actions-oidc)..."
aws cloudformation deploy `
  --template-file $Template `
  --stack-name stand-github-actions-oidc `
  --capabilities CAPABILITY_NAMED_IAM `
  --region us-east-1

$arn = aws cloudformation describe-stacks `
  --stack-name stand-github-actions-oidc `
  --query "Stacks[0].Outputs[?OutputKey=='RoleArn'].OutputValue" `
  --output text `
  --region us-east-1

Write-Host ""
Write-Host "Role ARN: $arn"
Write-Host ""
Write-Host "Next: add GitHub repo secret in adrisanpu/stand-backend:"
Write-Host "  Name:  AWS_ROLE_ARN_DEV"
Write-Host "  Value: $arn"
Write-Host ""
Write-Host "If gh CLI is installed:"
Write-Host "  gh secret set AWS_ROLE_ARN_DEV --repo adrisanpu/stand-backend --body `"$arn`""
