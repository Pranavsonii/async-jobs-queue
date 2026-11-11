# Runs through all the scenarios against the running stack
# (normal payout, duplicates, burst, DLQ, retries, webhooks, report).
#
#   docker compose up -d --build
#   ./scripts/demo.ps1

param([string]$BaseUrl = "http://localhost:8000")

$ErrorActionPreference = "Stop"

function Step($n, $title) {
    Write-Host ""
    Write-Host "── $n. $title " -ForegroundColor Cyan -NoNewline
    Write-Host ("─" * [Math]::Max(0, 60 - $title.Length)) -ForegroundColor DarkGray
}

function Post($path, $body) {
    Invoke-RestMethod -Uri "$BaseUrl$path" -Method Post `
        -ContentType "application/json" -Body ($body | ConvertTo-Json -Compress)
}

function Get-Json($path) { Invoke-RestMethod -Uri "$BaseUrl$path" }

function Show-Stats {
    $s = Get-Json "/stats"
    $depths = ($s.queues | ForEach-Object { "$($_.queue)=$($_.depth)" }) -join "  "
    Write-Host "   queues: $depths"
    Write-Host "   payouts: $($s.payouts_by_status | ConvertTo-Json -Compress)"
    Write-Host "   outbox_pending=$($s.outbox_pending)  dead_letters=$($s.dead_letters)  keys=$($s.idempotency_keys)"
}

Step 0 "Health — each dependency reported separately"
Get-Json "/health" | Format-List

Step 1 "A normal payout: 202 Accepted, then pending -> processing -> paid"
$ref = "DEMO-$(Get-Date -Format 'HHmmss')"
$accepted = Post "/payouts" @{reference = $ref; beneficiary = "Asha Kumar"; amount_paise = 250000 }
Write-Host "   accepted: $($accepted | ConvertTo-Json -Compress)"
Write-Host "   (the API never touched the gateway -- it wrote a row and returned)"

Start-Sleep -Seconds 8   # outbox poll (5s) + gateway latency
$p = Get-Json "/payouts/$($accepted.payout_id)"
Write-Host "   after 8s: status=$($p.status)  attempts=$($p.attempts)  gw=$($p.gateway_reference)"

Step 2 "Duplicate request: same reference, no second payout"
$again = Post "/payouts" @{reference = $ref; beneficiary = "Someone Else"; amount_paise = 999999 }
Write-Host "   created=$($again.created)  (false = deduplicated at the HTTP layer)"
$p = Get-Json "/payouts/$($accepted.payout_id)"
Write-Host "   attempts is still $($p.attempts), beneficiary is still '$($p.beneficiary)'"
Write-Host "   -> a retry is not an edit, and it is not a second payment"

Step 3 "Burst of 25: watch queue depth spike, then drain"
1..25 | ForEach-Object {
    Post "/payouts" @{
        reference    = "BURST-$(Get-Date -Format 'HHmmss')-$_"
        beneficiary  = "Beneficiary $_"
        amount_paise = 100000 + $_
    } | Out-Null
}
Write-Host "   enqueued 25"
Show-Stats
Write-Host "   waiting 15s for workers to drain..."
Start-Sleep -Seconds 15
Show-Stats

Step 4 "Poison message: permanent failure goes straight to the DLQ"
$poison = Post "/payouts" @{
    reference    = "FAIL-$(Get-Date -Format 'HHmmss')"
    beneficiary  = "Closed Account"
    amount_paise = 50000
}
Start-Sleep -Seconds 10
$p = Get-Json "/payouts/$($poison.payout_id)"
Write-Host "   status=$($p.status)  attempts=$($p.attempts)  reason='$($p.failure_reason)'"
Write-Host "   -> attempts=1: it did NOT burn five retries on an account that is closed"

$dlq = Get-Json "/dlq"
Write-Host "   DLQ holds $($dlq.Count) entry/entries, with full args + traceback:"
$dlq | Select-Object -First 1 | Format-List id, task_name, error

Step 5 "Replay from the DLQ after a 'fix'"
if ($dlq.Count -gt 0) {
    $entry = $dlq[0]
    Post "/dlq/$($entry.id)/replay" @{} | Out-Null
    Write-Host "   replayed dead letter $($entry.id)"
    Write-Host "   (it will fail again -- the reference still starts with FAIL- --"
    Write-Host "    but the point is the job was never lost and IS replayable)"
}

Step 6 "Transient failure: retries with backoff, then succeeds"
$flaky = Post "/payouts" @{
    reference    = "FLAKY-$(Get-Date -Format 'HHmmss')"
    beneficiary  = "Retry Demo"
    amount_paise = 75000
}
Write-Host "   enqueued a payout that always fails transiently"
Write-Host "   watch 'docker compose logs -f worker-payouts' -- backoff is 1s, 2s, 4s, 8s (+jitter)"
Start-Sleep -Seconds 12
$p = Get-Json "/payouts/$($flaky.payout_id)"
Write-Host "   status=$($p.status)  attempts=$($p.attempts)  <- attempts climbs with each retry"

Step 7 "Inbound webhook: deduplicated on the provider's event id"
$evt = "evt_$(Get-Date -Format 'HHmmss')"
$hook = @{
    event_id   = $evt
    event_type = "payout.processed"
    payload    = @{ reference = $ref; gateway_reference = "gw_from_webhook" }
}
Post "/webhooks/gateway" $hook | Out-Null
Post "/webhooks/gateway" $hook | Out-Null   # the provider's redelivery
Write-Host "   sent the SAME event twice (providers do this in production)"
Start-Sleep -Seconds 3
Write-Host "   second delivery was a no-op -- check worker-fast logs for 'duplicate, ignored'"

Step 8 "Fan-out / fan-in: chord-based statement on the heavy queue"
$report = Post "/reports/statement?window_days=7" @{}
Write-Host "   chord launched, poll id: $($report.task_id)"
Start-Sleep -Seconds 8
$status = Get-Json "/tasks/$($report.task_id)"
Write-Host "   state=$($status.state)  result=$($status.result | ConvertTo-Json -Compress)"

Step 9 "Final state"
Show-Stats
Write-Host ""
Write-Host "Flower:    http://localhost:5555" -ForegroundColor DarkGray
Write-Host "Dashboard: $BaseUrl" -ForegroundColor DarkGray
Write-Host ""
