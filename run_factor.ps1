param (
    [Parameter(Mandatory = $true)]
    [string]$factor
)

$proj_dir = "E:\Data\Projects\CTA_V6"
$bgn_date_factor = "20140102"
$bgn_date_qtest = "20150105"
$stp_date = "20250801"

Write-Host "Run for $factor"

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

python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass $factor
python main.py --bgn $bgn_date_qtest --stp $stp_date ic --fclass $factor
python main.py --bgn $bgn_date_qtest --stp $stp_date vt --fclass $factor
python main.py --bgn $bgn_date_qtest --stp $stp_date ot --fclass $factor
