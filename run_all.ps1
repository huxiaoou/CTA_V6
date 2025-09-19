Remove-Item E:\Data\Projects\CTA_V6\* -Recurse

$bgn_date_avlb = "20120104"
$bgn_date_factor = "20140102"
$bgn_date_qtest = "20150105"
$bgn_date_sig_fac = $bgn_date_qtest
$bgn_date_opt = "20161229" # must at least 2 days ahead of bgn date
$bgn_date_sig_stg = $bgn_date_opt
$bgn_date = "20170103"
$stp_date = "20250801"

python main.py --bgn $bgn_date_avlb --stp $stp_date available
python main.py --bgn $bgn_date_avlb --stp $stp_date market
python main.py --bgn $bgn_date_avlb --stp $stp_date css
python main.py --bgn $bgn_date_avlb --stp $stp_date icov
python main.py --bgn $bgn_date_avlb --stp $stp_date test_return

.\run_factor.ps1 $bgn_date_factor $bgn_date_qtest $stp_date BASIS
.\run_factor.ps1 $bgn_date_factor $bgn_date_qtest $stp_date RS
.\run_factor.ps1 $bgn_date_factor $bgn_date_qtest $stp_date KURT
.\run_factor.ps1 $bgn_date_factor $bgn_date_qtest $stp_date IKURT
.\run_factor.ps1 $bgn_date_factor $bgn_date_qtest $stp_date LIQUIDITY
.\run_factor.ps1 $bgn_date_factor $bgn_date_qtest $stp_date CTP
.\run_factor.ps1 $bgn_date_factor $bgn_date_qtest $stp_date CVP
.\run_factor.ps1 $bgn_date_factor $bgn_date_qtest $stp_date VAL
.\run_factor.ps1 $bgn_date_factor $bgn_date_qtest $stp_date NPLS
.\run_factor.ps1 $bgn_date_factor $bgn_date_qtest $stp_date TR

python main.py --bgn $bgn_date_sig_fac --stp $stp_date signals --type factors
python main.py --bgn $bgn_date_opt --stp $stp_date optimize
python main.py --bgn $bgn_date_sig_stg --stp $stp_date signals --type strategies
python main.py --bgn $bgn_date --stp $stp_date --nomp simulations
python main.py --bgn $bgn_date --stp $stp_date quick
