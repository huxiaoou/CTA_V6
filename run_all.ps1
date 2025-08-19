Remove-Item E:\Data\Projects\CTA_V6\* -Recurse

$bgn_date_avlb = "20120104"
$bgn_date_factor = "20140102"
$bgn_date_sig = "20161229" # must at least 2 days ahead of bgn date
$bgn_date = "20170109"
$stp_date = "20250701"

python main.py --bgn $bgn_date_avlb --stp $stp_date available
python main.py --bgn $bgn_date_avlb --stp $stp_date css
python main.py --bgn $bgn_date_avlb --stp $stp_date market
python main.py --bgn $bgn_date_avlb --stp $stp_date test_return

python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass BASIS
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass TS
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass RS
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass MTM
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass SKEW
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass KURT
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass LIQUIDITY
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass SIZE
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass CTP
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass CVP
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass MPH
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass VAL
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass ACR
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass REOC
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass NPLS
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass VENTROPY
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass AMP
python main.py --bgn $bgn_date_factor --stp $stp_date factor --fclass LCVR

python main.py --bgn $bgn_date --stp $stp_date ic --fclass BASIS
python main.py --bgn $bgn_date --stp $stp_date ic --fclass TS
python main.py --bgn $bgn_date --stp $stp_date ic --fclass RS
python main.py --bgn $bgn_date --stp $stp_date ic --fclass MTM
python main.py --bgn $bgn_date --stp $stp_date ic --fclass SKEW
python main.py --bgn $bgn_date --stp $stp_date ic --fclass KURT
python main.py --bgn $bgn_date --stp $stp_date ic --fclass LIQUIDITY
python main.py --bgn $bgn_date --stp $stp_date ic --fclass SIZE
python main.py --bgn $bgn_date --stp $stp_date ic --fclass CTP
python main.py --bgn $bgn_date --stp $stp_date ic --fclass CVP
python main.py --bgn $bgn_date --stp $stp_date ic --fclass MPH
python main.py --bgn $bgn_date --stp $stp_date ic --fclass VAL
python main.py --bgn $bgn_date --stp $stp_date ic --fclass ACR
python main.py --bgn $bgn_date --stp $stp_date ic --fclass REOC
python main.py --bgn $bgn_date --stp $stp_date ic --fclass NPLS
python main.py --bgn $bgn_date --stp $stp_date ic --fclass VENTROPY
python main.py --bgn $bgn_date --stp $stp_date ic --fclass AMP
python main.py --bgn $bgn_date --stp $stp_date ic --fclass LCVR

python main.py --bgn $bgn_date --stp $stp_date vt --fclass BASIS
python main.py --bgn $bgn_date --stp $stp_date vt --fclass TS
python main.py --bgn $bgn_date --stp $stp_date vt --fclass RS
python main.py --bgn $bgn_date --stp $stp_date vt --fclass MTM
python main.py --bgn $bgn_date --stp $stp_date vt --fclass SKEW
python main.py --bgn $bgn_date --stp $stp_date vt --fclass KURT
python main.py --bgn $bgn_date --stp $stp_date vt --fclass LIQUIDITY
python main.py --bgn $bgn_date --stp $stp_date vt --fclass SIZE
python main.py --bgn $bgn_date --stp $stp_date vt --fclass CTP
python main.py --bgn $bgn_date --stp $stp_date vt --fclass CVP
python main.py --bgn $bgn_date --stp $stp_date vt --fclass MPH
python main.py --bgn $bgn_date --stp $stp_date vt --fclass VAL
python main.py --bgn $bgn_date --stp $stp_date vt --fclass ACR
python main.py --bgn $bgn_date --stp $stp_date vt --fclass REOC
python main.py --bgn $bgn_date --stp $stp_date vt --fclass NPLS
python main.py --bgn $bgn_date --stp $stp_date vt --fclass VENTROPY
python main.py --bgn $bgn_date --stp $stp_date vt --fclass AMP
python main.py --bgn $bgn_date --stp $stp_date vt --fclass LCVR

python main.py --bgn $bgn_date_sig --stp $stp_date signals --type factors
python main.py --bgn $bgn_date_sig --stp $stp_date optimize
python main.py --bgn $bgn_date_sig --stp $stp_date signals --type strategies
#python main.py --bgn $bgn_date --stp $stp_date quick
python main.py --bgn $bgn_date --stp $stp_date --nomp simulations --type strategies
