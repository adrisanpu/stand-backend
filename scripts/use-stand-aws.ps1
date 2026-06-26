# Sets AWS_PROFILE for Stand deploy/CLI work in this shell. One-time: aws configure --profile stand-deploy-cli
$Profile = "stand-deploy-cli"
$env:AWS_PROFILE = $Profile
$env:AWS_DEFAULT_REGION = "us-east-1"
Write-Host "AWS_PROFILE=$Profile (region us-east-1)"
aws sts get-caller-identity
if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "Profile not configured. Run once:"
    Write-Host "  aws configure --profile $Profile"
    Write-Host "Use access keys for IAM user stand-deploy-cli (region: us-east-1, output: json)."
    exit 1
}
