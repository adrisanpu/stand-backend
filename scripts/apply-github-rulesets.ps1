# Apply dev-branch squash ruleset to Stand repos (requires gh CLI + auth)
$ErrorActionPreference = "Stop"
$Ruleset = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "..\docs\github-ruleset-dev-squash.json"
$Repos = @("stand-backend", "stand-frontend", "stand-website")

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
  Write-Error "gh CLI not found. Install from https://cli.github.com/ then: gh auth login"
}

foreach ($repo in $Repos) {
  $full = "adrisanpu/$repo"
  Write-Host "Applying ruleset to $full ..."
  gh api "repos/$full/rulesets" --method POST --input $Ruleset
}

Write-Host "Done. Verify in each repo: Settings -> Rules -> Rulesets -> dev-squash-merge"
