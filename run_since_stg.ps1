param (
    [Parameter(Mandatory = $false)]
    [Switch] $EnableSig
)

$proj_dir = "E:\Data\Projects\CTA_V6"
$bgn_date_qtest = "20150105"
$bgn_date_sig_fac = $bgn_date_qtest
$bgn_date_opt = "20161229" # must at least 2 days ahead of bgn date
$bgn_date_sig_stg = $bgn_date_opt
$bgn_date = "20170103"
$stp_date = "20250801"

if ($EnableSig) {
    Remove-Item "$proj_dir\signals_factors\*" -Recurse
    python main.py --bgn $bgn_date_sig_fac --stp $stp_date signals --type factors
}

Remove-Item "$proj_dir\optimize\*" -Recurse
Remove-Item "$proj_dir\signals_strategies\*" -Recurse
Remove-Item "$proj_dir\simulations\*" -Recurse
Remove-Item "$proj_dir\evaluations\*" -Recurse
Remove-Item "$proj_dir\sims_quick\*" -Recurse

python main.py --bgn $bgn_date_opt --stp $stp_date optimize
python main.py --bgn $bgn_date_sig_stg --stp $stp_date signals --type strategies
python main.py --bgn $bgn_date --stp $stp_date --nomp simulations
python main.py --bgn $bgn_date --stp $stp_date quick
