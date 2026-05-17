# Squash-only merge on Stand repos (GitHub Free — no branch rulesets on private repos).
# Requires gh CLI: https://cli.github.com/
$ErrorActionPreference = "Stop"
$Repos = @("stand-backend", "stand-frontend", "stand-website")

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
  Write-Error "gh CLI not found. Install from https://cli.github.com/ then: gh auth login"
}

foreach ($repo in $Repos) {
  $full = "adrisanpu/$repo"
  Write-Host "Setting squash-only merge on $full ..."
  gh api "repos/$full" -X PATCH `
    -f allow_squash_merge=true `
    -f allow_merge_commit=false `
    -f allow_rebase_merge=false
}

Write-Host "Done. All PRs in these repos must use squash merge."
