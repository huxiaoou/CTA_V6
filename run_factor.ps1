param (
    [Parameter(Mandatory = $true)]
    [string]$bgn_date_factor,
    [string]$bgn_date_qtest,
    [string]$stp_date,
    [string]$factor,
    [Switch]$DisableMP
)

$proj_dir = "E:\Data\Projects\CTA_V6"

Write-Host "Run for $factor, factor = $bgn_date_factor, qtest = $bgn_date_qtest, stp = $stp_date"

$factor_dir = "$proj_dir\factors_by_instru\$factor"
if (Test-Path $factor_dir)
{
    Remove-Item $factor_dir -Recurse
}
Remove-Item "$proj_dir\factors_avlb_raw\$factor-*.db"
Remove-Item "$proj_dir\factors_avlb_ewa\$factor-*.db"

Remove-Item "$proj_dir\ic_tests\data\$factor-*.db"
Remove-Item "$proj_dir\ic_tests\plots\$factor-*.pdf"
Remove-Item "$proj_dir\ic_tests\\$factor-*.csv"

Remove-Item "$proj_dir\vt_tests\data\$factor-*.db"
Remove-Item "$proj_dir\vt_tests\plots\$factor-*.pdf"
Remove-Item "$proj_dir\vt_tests\\$factor-*.csv"

Remove-Item "$proj_dir\ot_tests\data\$factor-*.db"
Remove-Item "$proj_dir\ot_tests\plots\$factor-*.pdf"
Remove-Item "$proj_dir\ot_tests\\$factor-*.csv"

if ($DisableMP)
{
    python main.py --bgn $bgn_date_factor --stp $stp_date --nomp factor --fclass $factor
    python main.py --bgn $bgn_date_qtest --stp $stp_date --nomp ic --fclass $factor
    python main.py --bgn $bgn_date_qtest --stp $stp_date --nomp vt --fclass $factor
    python main.py --bgn $bgn_date_qtest --stp $stp_date --nomp ot --fclass $factor
}
else
{
    python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass $factor
    python main.py --bgn $bgn_date_qtest --stp $stp_date ic --fclass $factor
    python main.py --bgn $bgn_date_qtest --stp $stp_date vt --fclass $factor
    python main.py --bgn $bgn_date_qtest --stp $stp_date ot --fclass $factor
}
