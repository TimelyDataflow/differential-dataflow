extern crate timely;
extern crate differential_dataflow;

// taken from: https://adventofcode.com/2017/day/8

// use differential_dataflow::Collection;
use differential_dataflow::input::Input;
use differential_dataflow::operators::*;
use differential_dataflow::algorithms::prefix_sum::PrefixSum;

fn main() {

    let input = "ebu inc 626 if iq < 0
obc dec -809 if sdw == -2
vb inc 568 if k > -2
wl dec 721 if wui <= 2
xbh dec -310 if bx != 1
qun inc 741 if qun > -4
k dec 661 if sdw != 0
wui inc 628 if k >= -4
qet inc 563 if skh == 0
ebu dec 197 if wl < -716
qun dec 239 if sdw < 7
n dec 655 if azg == 0
iq inc -857 if kxm <= -9
qet inc -959 if tzy > 5
azg inc -643 if qun >= 510
ebu inc 537 if c >= -8
eh inc -677 if bx >= -1
c dec -267 if ebu < 341
sdw dec 811 if tzy != -1
wui inc -775 if qet >= 562
iq inc 215 if azg >= -8
qun inc 949 if sdw > -813
xjo inc -652 if vb != 563
skh inc -560 if n < -645
xjo dec 978 if ne == 5
skh dec 913 if k > -6
iq inc 783 if azg != 1
skh inc 955 if ne == 0
xbh inc -981 if n > -660
eh inc -361 if vb == 578
iq dec 304 if obc != -3
eh inc 408 if tzy >= -2
w inc -576 if tzy == 0
iq dec 102 if udh < 7
k inc -627 if qet > 558
xjo inc -232 if ne == 0
azg dec -739 if j > -5
a dec -141 if qet == 567
a dec -731 if a < 2
i dec -839 if as < 8
v dec 855 if xbh >= -672
wl dec -602 if wl == -721
obc inc -613 if ebu == 340
j inc 605 if wui >= -148
kxm dec -192 if skh > -521
skh dec 551 if skh >= -512
xbh dec 340 if bx != -9
qet inc 156 if w <= -575
azg dec 627 if eh < -276
qet inc 7 if ne != 5
vjx inc -922 if as == 0
vjx inc -680 if k <= -623
as dec -807 if w != -576
tzy inc -808 if qun != 1458
wui dec -905 if a <= 740
bx inc 371 if n != -655
xbh inc 721 if n != -664
xbh inc 79 if w == -576
udh dec 217 if azg > 738
eh dec 300 if a > 721
k inc 684 if xbh >= -215
i inc -281 if qet >= 717
v dec -408 if kxm > 187
as inc 923 if xbh <= -202
v dec 640 if vjx < -1592
kxm inc 537 if xbh < -215
xbh dec 426 if eh == -564
obc dec -269 if c < 272
udh dec -256 if wl == -119
xbh inc -519 if v <= -1083
eh inc 134 if wl >= -117
azg dec 485 if eh >= -576
obc dec -134 if as != 925
wui inc 549 if udh < 42
eh dec -323 if obc >= -219
a dec 553 if w < -575
vjx dec 311 if qet == 726
vjx dec 602 if as != 930
tzy dec -588 if udh < 41
i inc 479 if eh < -238
eh inc 602 if j != 609
wl inc -800 if sdw == -811
qet dec -493 if k >= 48
obc dec -469 if v >= -1084
ne dec 100 if skh == -518
c inc 574 if tzy >= -225
wui dec -390 if bx > -7
ebu dec 786 if v < -1079
kxm inc -637 if i == 1037
as dec 378 if w == -576
ne dec -946 if v <= -1092
udh inc -433 if obc <= -220
azg dec 660 if w >= -581
v inc 61 if qun == 1451
v inc 644 if v <= -1025
udh inc 531 if k >= 52
obc inc 381 if n <= -655
xjo inc -485 if a != 174
tzy dec -526 if obc > 179
azg inc -831 if skh == -518
sdw dec 288 if sdw <= -808
kxm inc 759 if kxm != -451
n inc -847 if c > 839
skh dec 384 if tzy != -220
i dec -532 if obc > 163
a inc 570 if eh == 356
c dec 437 if wui > 1687
i inc -403 if vb >= 564
wui dec -223 if azg >= -1238
vb dec 370 if vb != 574
qun inc -251 if skh == -518
n inc 490 if iq < 594
kxm inc -542 if v >= -376
c dec 49 if kxm == 314
a inc -970 if xbh >= -734
skh inc -258 if j == 605
kxm dec 180 if j == 605
j dec 243 if k >= 54
vb inc 832 if azg >= -1231
udh dec 662 if tzy >= -229
c inc -327 if bx != -6
obc dec 973 if kxm == 128
udh dec 543 if vb <= 196
n inc 534 if qun < 1208
xjo dec -734 if eh < 362
j dec 629 if azg == -1237
wl dec -205 if qet <= 1219
qet dec -811 if a <= -214
azg inc -936 if sdw >= -1099
udh dec 222 if azg < -2172
j inc -526 if obc < 174
ebu inc -725 if n >= -484
eh inc 120 if i == 1166
wl dec 292 if n >= -487
k inc -718 if c == 28
qet inc -472 if ne < -109
ne inc -426 if qet == 2030
a dec 351 if iq < 600
sdw inc -446 if qet >= 2023
i inc 666 if qun <= 1194
obc dec -915 if udh > -315
tzy inc 281 if a == -573
udh inc -18 if as == 545
k inc 767 if as >= 552
as inc 84 if xjo != -641
vjx dec -321 if qun != 1204
v inc 799 if i > 1162
qet dec -575 if obc > 1079
vb dec -671 if obc < 1077
bx inc -976 if vb == 198
tzy dec 316 if qun == 1200
skh dec -352 if qet <= 2600
sdw dec 407 if qet <= 2605
c dec -398 if iq != 597
i dec 943 if a < -569
wui dec -455 if as != 625
skh inc 392 if k > -668
qun inc 162 if k != -667
xbh dec -311 if sdw == -1962
sdw dec 219 if wl < -1012
vb inc 610 if qun < 1372
bx dec 314 if azg != -2170
xjo inc 298 if wl >= -1007
iq dec -229 if vjx != -2194
obc dec -71 if kxm >= 127
k dec 936 if n > -485
azg dec -282 if v >= 417
j dec 8 if qet < 2614
j dec 599 if tzy >= -254
w inc -111 if v >= 416
skh inc -365 if wui <= 2375
vb inc 830 if qun <= 1362
bx dec -992 if qun >= 1366
obc inc -254 if obc != 1150
azg dec -557 if udh >= -326
c inc -438 if sdw >= -1953
eh inc 41 if as < 635
eh inc -725 if sdw != -1958
azg dec 515 if vb <= 1640
vb dec 149 if qet >= 2597
a inc 431 if wui < 2381
qun inc 596 if c >= -14
i dec -520 if a != -151
v dec -500 if j == -803
wui inc -581 if vjx == -2204
vb inc -753 if a < -139
tzy inc 309 if j > -796
xbh inc -182 if w > -685
qun dec -315 if j < -792
as dec 688 if eh < -202
bx inc -348 if ne > -533
n dec 198 if w >= -690
eh inc 179 if azg == -2406
qun dec -636 if eh == -19
skh dec 684 if a == -137
i dec -501 if wl < -1005
w inc -894 if obc >= 903
qun dec 529 if ebu < -1172
sdw inc -303 if eh != -34
wui inc -912 if bx >= -1630
wui inc 222 if bx != -1643
ebu inc -907 if c < -17
tzy inc -871 if tzy > -256
bx inc -119 if iq > 591
udh dec 56 if n > -678
qun inc -839 if vjx <= -2189
udh inc 867 if j < -791
w inc 26 if i <= 1247
i inc 633 if vb <= 745
v inc -384 if skh >= -757
ebu inc 876 if ne >= -529
udh inc 116 if a <= -145
a dec 179 if a >= -146
ne dec 643 if c == -12
c dec -490 if bx > -1764
qet inc 286 if ebu > -291
as dec -213 if i < 1884
sdw inc -80 if wui != 2597
xjo inc 59 if azg != -2406
n inc 640 if azg > -2399
k dec 444 if wl >= -1006
sdw inc -45 if qun != 1437
kxm inc -330 if bx >= -1757
wui inc 934 if i == 1884
qet dec 295 if azg > -2416
j inc 435 if xbh > -736
a inc -454 if eh >= -36
obc dec -533 if iq == 592
w inc 388 if ne > -1170
wui inc 866 if udh >= 474
wl inc -858 if vb != 736
wl inc -90 if vb < 745
wui dec 794 if vb >= 727
w dec -198 if eh <= -22
n dec 203 if qun != 1431
a inc 252 if bx <= -1757
udh dec 589 if vjx != -2194
azg inc 10 if azg < -2403
vb inc -293 if xbh > -739
udh inc -737 if a != -519
j dec -582 if obc >= 1433
xjo dec -892 if xjo == -337
qun inc 668 if obc <= 1443
udh inc -124 if bx == -1757
azg inc -210 if wui < 2665
i inc -312 if wl > -1087
ebu dec 356 if k > -2041
qet inc 928 if skh == -749
tzy inc 299 if i != 1874
vb dec 151 if vb <= 447
qun dec 936 if eh < -25
eh inc 782 if vjx < -2191
n inc 515 if j < 226
udh inc 482 if obc > 1434
azg dec -244 if n < -356
n inc -192 if kxm == -196
wl inc -573 if bx > -1760
sdw inc 423 if vjx >= -2195
c dec -72 if wui <= 2670
k inc 984 if a == -523
eh inc -288 if a > -525
ebu dec 855 if a < -514
kxm dec -261 if a > -533
wui inc -58 if udh == 100
sdw inc -304 if skh <= -742
vjx dec -86 if qet < 3247
w dec 163 if v >= 32
kxm inc 231 if ne != -1169
vb inc -291 if udh <= 97
v dec 759 if iq >= 584
xjo inc 390 if xbh < -728
bx dec -760 if j > 215
wl dec 370 if kxm > 57
kxm inc 871 if vb >= 285
c dec 4 if n > -556
i dec 813 if v > -734
v dec 129 if udh <= 100
qun dec -762 if ebu == -1150
v dec -415 if as >= 151
ne dec 366 if obc == 1440
udh inc 298 if a <= -533
tzy inc -245 if wl != -2038
i dec 992 if as > 151
c inc -400 if a == -523
wl dec 317 if qun < 1933
azg dec 174 if tzy < -1067
vb dec -780 if vb < 302
udh dec 273 if ne < -1166
obc inc -669 if wl <= -2354
as dec 843 if k >= -1064
as inc 902 if kxm < 944
qun inc -154 if i >= 69
j dec -894 if azg > -2334
as inc 233 if w > -1140
xjo dec 14 if xjo >= 940
sdw dec 999 if tzy != -1066
qun inc -351 if wl > -2363
kxm inc -503 if qet >= 3230
kxm dec -843 if bx != -1003
obc dec -318 if sdw > -3187
wl inc -243 if n >= -564
a inc -873 if sdw < -3179
bx dec 602 if wl >= -2601
wui dec 237 if c >= 145
wui inc -999 if qun >= 1420
w dec 329 if bx != -1599
a inc -550 if xbh > -735
ebu inc 359 if azg != -2328
tzy inc 965 if tzy >= -1067
wui inc -608 if c >= 141
vjx inc -168 if as <= 447
qun dec -183 if qet < 3241
xbh inc -456 if tzy <= -1069
j dec 581 if j != 1110
qun inc 677 if udh >= -173
udh dec 783 if xjo > 924
kxm inc 775 if w == -1132
k dec -961 if v == -440
qet dec 970 if skh != -753
qet dec 566 if vb >= 1064
sdw inc -639 if j == 1110
ebu dec -22 if kxm <= 2051
as dec 9 if tzy == -1072
w inc -513 if obc < 1083
k dec 669 if sdw <= -3822
obc inc -795 if c >= 142
n inc -980 if bx == -1599
obc inc 176 if qet <= 1697
j inc 517 if tzy != -1065
wui inc 394 if qun != 2276
tzy dec -833 if ne == -1170
eh dec 112 if k < -88
wui inc -648 if udh > -964
wl dec 918 if w < -1125
qun dec 846 if skh >= -755
sdw dec 287 if tzy == -1064
udh inc 902 if as != 443
skh dec 793 if ebu > -777
w dec 689 if j > 1621
j dec 415 if azg == -2326
obc dec -869 if k > -101
c dec -524 if eh == 353
j dec 519 if a > -1949
n inc -811 if xbh > -1193
udh dec 980 if qet == 1702
iq inc 358 if qun == 1428
w dec -719 if vb <= 1076
wl inc -543 if vjx >= -2284
vb inc -778 if wui == 517
iq inc -611 if azg != -2330
xbh dec 716 if azg < -2326
obc inc 997 if tzy > -1067
vb inc -645 if kxm <= 2058
iq dec -703 if sdw != -3819
xjo inc -766 if xjo < 924
obc inc 345 if c > 670
i dec 435 if qet > 1697
kxm inc 895 if ne <= -1173
kxm inc 387 if udh < -1041
k dec 851 if wui <= 508
vjx inc -957 if iq >= -26
wui dec -102 if wui < 523
ebu inc -441 if qet < 1710
qet dec -774 if vjx < -3224
skh inc -554 if qun >= 1431
xbh dec -924 if sdw < -3816
a dec 250 if qun <= 1438
xjo dec -430 if wui != 619
sdw inc -845 if kxm >= 2044
udh dec 607 if a <= -2191
w inc -940 if v >= -440
w inc -224 if bx == -1599
ne dec 856 if v != -441
tzy inc 431 if vjx == -3233
azg inc 370 if skh <= -2091
tzy inc 530 if qet > 2468
w inc 371 if vjx != -3233
kxm inc 348 if azg < -1959
n inc -611 if a <= -2190
k inc 365 if c < 681
kxm dec -734 if skh <= -2093
n inc 243 if w == -2266
skh inc 537 if j > 684
j dec -127 if wl <= -4066
n inc -903 if a > -2199
udh inc 685 if kxm == 2785
j inc 656 if v > -435
vb dec 193 if bx == -1599
eh inc -251 if ebu == -1210
qun inc 882 if ebu > -1215
k dec 705 if w == -2266
xbh inc -92 if sdw != -4657
w dec -689 if iq != -19
qun inc 962 if sdw > -4671
wui inc -476 if obc == 1504
azg inc 946 if i > -360
ebu inc 50 if n <= -3616
iq inc -284 if a == -2196
vjx inc -470 if v == -440
w dec 888 if ebu < -1151
a dec 369 if ebu <= -1153
udh dec 3 if kxm > 2777
qet dec -606 if tzy <= -106
kxm inc -483 if sdw <= -4661
vb dec 706 if tzy > -120
udh inc 111 if kxm >= 2296
iq dec 77 if xbh == -354
tzy dec -534 if sdw != -4655
tzy dec -300 if tzy >= 421
ne dec 258 if skh <= -1552
qet dec 363 if i == -357
ebu dec -16 if bx != -1591
eh inc -420 if a <= -2567
qet inc 674 if azg >= -1962
k inc 891 if azg != -1956
ne dec 730 if wl < -4058
ebu dec 497 if wui != 147
udh inc -969 if j <= 699
tzy dec 536 if eh != 101
xjo inc -70 if tzy < 193
xjo dec -873 if tzy < 188
udh inc -923 if k != -442
qet dec -56 if tzy != 193
wui dec -982 if sdw < -4661
iq dec 690 if qet > 3805
azg inc -961 if i > -360
udh dec 856 if wui != 1113
bx inc -710 if j <= 701
n inc 914 if i < -360
eh dec -772 if qet != 3812
ne dec 70 if vb < -470
j dec -460 if i <= -355
qun dec -837 if c != 674
w inc 961 if azg <= -1948
ebu dec -192 if k > -439
w inc 795 if xbh >= -359
wl inc 68 if v <= -436
eh inc 555 if qun >= 3288
vb inc 322 if eh != 95
xjo inc 995 if sdw != -4669
k dec 623 if w != -1388
udh inc 533 if azg > -1960
xbh dec 700 if ebu <= -1441
xbh dec 570 if udh > -3070
vb dec -434 if w == -1398
as inc 173 if bx != -2300
w inc 634 if bx == -2309
vjx dec -719 if i <= -361
w dec -796 if c <= 677
i dec -410 if azg != -1956
udh inc -738 if as != 602
vjx inc -617 if ebu <= -1446
ne dec -518 if ne == -3091
vjx dec 135 if n == -2704
wl dec 78 if tzy == 187
tzy dec 753 if wl > -4075
ebu dec 679 if c >= 683
tzy dec -276 if w <= 34
a dec 82 if wui < 1126
skh inc 473 if azg < -1948
udh inc -26 if tzy < -289
xjo inc 407 if eh == 102
i dec 909 if kxm >= 2306
udh dec -97 if xjo > 3564
tzy dec -858 if eh != 110
ne inc -102 if c != 678
iq dec -655 if iq >= -1063
tzy dec -283 if obc <= 1497
vjx dec 874 if udh < -3728
xjo dec -521 if tzy > 576
ne dec -532 if skh == -1086
j dec 316 if i != -362
sdw dec -256 if vjx >= -4612
ebu dec -695 if v > -443
azg dec 154 if k >= -1062
vjx dec 933 if sdw <= -4399
bx inc 696 if iq > -1069
skh inc -130 if wui <= 1126
eh dec -961 if c >= 669
bx inc 482 if k >= -1061
vb dec 177 if wui == 1121
n dec 169 if i == -355
eh dec 29 if xjo == 3558
xjo inc 59 if xbh == -1624
xjo inc 435 if wl <= -4074
udh dec -642 if skh >= -1224
iq dec 794 if udh != -3098
eh dec -962 if c > 673
wui inc 489 if as <= 613
iq dec -353 if v <= -435
iq inc -293 if n == -2704
n dec -667 if vb > 102
j dec 763 if eh == 2025
kxm inc 100 if i >= -367
tzy inc 757 if j <= 82
i inc -902 if i != -359
ne inc 3 if i < -1261
vb dec 272 if qet != 3818
skh dec -348 if j <= 76
w dec 399 if eh == 2027
skh inc 995 if c != 680
wl inc 343 if a > -2653
eh dec 628 if eh >= 2017
w dec 544 if azg >= -2116
bx inc 54 if xbh != -1624
as dec -724 if vb == -165
tzy dec 48 if vjx == -5543
sdw inc -837 if as != 1333
i inc 692 if obc != 1496
k dec -395 if tzy > 1283
ne dec -925 if vjx > -5549
xbh dec -670 if wl > -3735
obc dec 947 if wui >= 1619
k dec -480 if sdw != -5245
a inc -919 if vb > -158
wui inc 8 if qet == 3812
i inc -855 if eh == 1397
azg inc -41 if j > 74
xbh dec -945 if v > -431
xjo dec -95 if iq >= -1802
w dec -122 if kxm <= 2405
xjo inc -721 if bx >= -1833
xbh dec -182 if ebu < -751
obc dec 694 if wui >= 1614
tzy inc 755 if qet <= 3815
vjx dec -537 if c < 682
v dec 631 if qun > 3275
j dec -573 if xjo >= 2895
ebu dec 592 if xbh <= -776
xjo dec -748 if skh == 128
iq inc 30 if j < 657
bx dec 693 if sdw <= -5240
wui inc -953 if qet < 3817
qet inc 969 if xjo > 2909
as dec 963 if as < 1344
wl inc -997 if as != 372
xbh dec 514 if w <= -383
tzy inc 343 if xjo <= 2905
azg inc 470 if ne < -1725
kxm dec 126 if bx >= -2527
vjx dec -213 if c != 684
wl dec -491 if as >= 377
a dec -769 if azg <= -2114
obc dec 576 if qun >= 3286
udh inc 227 if obc != 805
ne inc 528 if v < -1062
as inc 124 if skh <= 135
a inc -896 if skh <= 131
k dec 949 if ebu == -754
c dec -541 if iq > -1783
wl dec -662 if udh > -2863
xjo inc -41 if qun != 3284
n dec -486 if iq > -1775
vjx dec 982 if obc > 803
kxm dec 349 if azg >= -2115
ebu dec -19 if bx >= -2515
xbh dec 199 if kxm < 1937
ebu dec -907 if wl > -4066
w inc 196 if bx == -2520
bx inc -534 if udh > -2871
skh inc 296 if iq < -1777
azg inc 169 if v < -1063
ne inc -187 if qet >= 3820
qun dec -15 if skh != 130
skh inc -909 if c < 1220
obc dec 583 if obc < 812
ebu dec -879 if qun < 3298
a dec 818 if obc >= 219
sdw dec 169 if as < 505
as inc 971 if sdw != -5422
wl dec 453 if a <= -4361
a dec 584 if udh > -2866
n dec 486 if w != -192
w dec -423 if iq > -1784
ebu inc 681 if as <= 1470
skh dec -585 if sdw <= -5421
qun dec -569 if n != -2038
bx inc -705 if qun >= 3857
w inc -198 if iq <= -1773
ne inc -345 if k <= -2005
vjx inc -368 if n > -2044
eh dec -267 if v == -1071
j inc 959 if c != 1222
c dec -544 if ebu < 1718
c dec -14 if xbh >= -1490
eh dec -746 if as <= 1458
azg dec 746 if tzy <= 2376
a inc -306 if c >= 1781
i inc 655 if azg == -2692
a inc -201 if eh >= 1666
bx inc 426 if qet == 3812
n dec 234 if c < 1776
azg dec -43 if qun < 3867
a dec 527 if a == -4945
iq dec 317 if qun != 3865
iq dec -685 if ne >= -1547
azg inc 309 if v == -1071
as inc 269 if udh <= -2866
a dec 764 if obc == 227
eh inc 918 if qun > 3861
udh dec 450 if wl <= -4509
eh dec -56 if j <= 1615
wui dec -557 if wl >= -4519
skh dec 479 if bx > -3340
skh dec 359 if kxm > 1917
as dec -754 if xjo <= 2868
eh inc -50 if wui >= 1213
xjo dec 883 if j != 1606
qet inc -687 if vjx < -6145
k dec 899 if udh != -3318
azg dec 242 if ebu != 1710
w dec -837 if as == 2217
ne dec 419 if obc > 225
skh dec -258 if qet != 3820
kxm dec -149 if azg != -2581
qet inc 264 if tzy == 2381
xjo dec -46 if qun <= 3866
w dec -772 if ebu > 1705
eh inc 7 if ne > -1955
xbh dec 149 if vb > -159
w inc 830 if xjo >= 2906
xbh dec -341 if v < -1062
obc dec -849 if w == 1633
qet dec 632 if ne >= -1959
j inc 599 if bx >= -3333
ebu dec -326 if vjx != -6133
i inc 752 if udh == -3313
iq dec 305 if skh < -1366
k dec 641 if eh == 2588
vb inc -652 if eh < 2597
skh inc -926 if v >= -1076
j inc -59 if k > -3556
wl dec 958 if wui < 1230
obc inc 871 if xjo < 2919
j inc -220 if k < -3540
c dec -411 if qet >= 3806
wl dec 874 if w < 1634
skh dec 285 if udh <= -3306
udh inc 627 if i != -1431
bx dec -633 if xbh != -1140
i inc -815 if wl >= -6354
c inc 892 if qet > 3811
wl inc 794 if azg != -2568
obc inc 103 if skh < -2566
k inc -188 if ebu < 2049
k dec -356 if vjx != -6149
sdw inc -314 if k > -3384
wl inc 198 if j < 1934
a inc -533 if i != -2251
ne dec -231 if iq <= -1084
ne inc -339 if tzy <= 2382
kxm dec 641 if w != 1640
iq dec 492 if skh <= -2567
sdw dec -229 if obc >= 2047
skh inc -194 if ebu <= 2041
xjo inc 275 if azg >= -2583
iq dec 826 if obc == 2053
sdw dec -993 if kxm > 1444
obc dec 15 if ebu == 2035
skh inc 297 if c <= 3080
sdw dec 748 if udh == -2684
v inc 793 if c <= 3083
obc dec -62 if qun >= 3861
vjx inc -703 if xjo >= 3188
wl dec 68 if ne >= -2077
udh inc 199 if iq < -1579
azg inc 991 if n == -2271
bx inc -554 if vb != -812
w dec -754 if kxm != 1445
vb inc -301 if iq <= -1582
udh inc 849 if wui < 1231
qun dec 495 if xbh == -1144
n dec -349 if w == 2387
n inc 172 if wl < -5416
wui inc -192 if wl <= -5426
vb dec 676 if kxm != 1436
n inc -365 if as >= 2224
eh dec -938 if vb != -1492
j dec -936 if j >= 1923
eh inc -889 if qet != 3802
tzy inc 629 if iq <= -1574
bx dec -333 if c <= 3073
j inc 118 if c <= 3085
bx inc 217 if eh >= 2640
xbh dec -587 if wl >= -5428
kxm inc 9 if tzy != 3011
qet inc 433 if c != 3074
tzy dec 408 if obc > 2108
ne inc -17 if vb >= -1502
tzy dec -223 if azg == -1586
azg dec -925 if azg >= -1580
bx dec -645 if tzy >= 2823
eh dec 870 if sdw > -6252
obc dec -355 if udh != -1645
ne inc -191 if obc != 2477
c inc -88 if wl < -5417
skh inc -36 if sdw > -6251
w inc -404 if sdw <= -6247
as inc 126 if vb <= -1491
iq dec -456 if c != 2991
skh dec -407 if v > -278
a dec 621 if iq > -1135
xjo dec -514 if kxm != 1447
wui dec -912 if iq != -1124
xjo dec 373 if c == 2982
ne inc 61 if ebu != 2048
qet inc -691 if iq == -1125
as inc 553 if vjx > -6148
qun inc -479 if qun == 3370
tzy dec -851 if ebu == 2039
xbh inc -52 if skh >= -2514
udh dec -853 if wl == -5423
udh dec 868 if i != -2245
wl inc 665 if w < 1986
qet inc -975 if w == 1983
kxm dec 677 if kxm < 1446
i dec 964 if j != 2978
i dec 719 if a <= -7385
w inc 142 if qun > 2887
xbh inc 547 if sdw >= -6256
ebu inc 102 if azg > -1592
i inc -255 if v <= -272
udh inc 681 if c < 2987
a inc 535 if qun >= 2887
udh dec -66 if xjo < 3707
vjx inc -496 if j != 2985
tzy inc -131 if v == -278
wui inc 414 if azg >= -1579
vb dec -579 if qun < 2896
v inc -275 if kxm != 767
vb inc -848 if w > 2127
j dec -601 if vjx > -6641
bx dec 311 if wl < -4756
n dec -995 if kxm != 764
iq inc -35 if sdw == -6247
sdw inc 153 if v >= -271
iq dec 219 if j == 3589
tzy dec 183 if j > 3577
a inc 193 if a > -6858
qet inc -847 if xjo >= 3697
iq dec -515 if n <= -755
j inc -690 if j == 3581
v dec -478 if a == -6662
n inc 602 if wl < -4753
xjo dec 532 if ne > -2213
i inc 958 if bx != -3556
k inc -587 if a <= -6662
c dec -82 if eh > 1763
vjx inc 668 if j < 2891
ebu inc -357 if as < 2906
n dec -283 if ne == -2216
w dec 778 if iq <= -640
vb dec 408 if ebu < 1784
azg dec 271 if vjx != -6639
udh dec -46 if ebu != 1784
k dec 628 if azg >= -1592
vb inc -962 if obc < 2467
wl dec 408 if v != 198
a dec 554 if vjx < -6631
c inc 442 if udh > -1591
i dec -417 if eh >= 1763
qet dec -856 if c > 3519
iq dec -144 if as < 2899
sdw inc -229 if skh != -2507
i dec -920 if i < -2800
qet dec -244 if n != 138
vb dec 811 if k != -4600
k inc 560 if qet != 1976
v dec 42 if w != 1342
a inc -588 if skh > -2514
qet inc 56 if obc < 2472
i inc -870 if eh < 1770
ebu dec -915 if w >= 1345
xbh dec 455 if i <= -2752
xbh dec 384 if xjo <= 3699
wl inc -895 if xjo != 3692
kxm inc -190 if sdw <= -6475
vjx dec 450 if xjo != 3698
skh dec 0 if skh > -2514
xjo dec 896 if udh >= -1589
udh inc 811 if k > -4587
eh inc 520 if n != 130
iq inc 873 if i == -2756
eh dec -76 if skh == -2506
ne inc -833 if wl <= -6057
tzy inc -687 if xbh == -901
n dec -232 if iq >= 223
vjx inc -968 if qet == 2032
as inc 503 if c == 3512
a inc 952 if qun != 2892
c dec -290 if wui >= 2128
a dec -777 if wui != 2140
qun inc -472 if tzy >= 2664
vb inc -589 if qet < 2041
tzy dec -548 if v > 153
wui inc 152 if ebu <= 2706
w dec 905 if tzy != 3224
udh dec -241 if obc <= 2473
v dec -532 if sdw <= -6473
bx inc 818 if wui == 2281
skh dec 547 if a != -6070
qet inc 146 if a <= -6067
n inc -528 if iq != 232
qet dec -864 if ne < -3044
qet inc -121 if azg < -1583
i inc 363 if vjx > -7609
k dec -105 if ne > -3040
a inc 480 if i <= -2398
udh inc 173 if ne == -3049
n inc -712 if as < 3405
j dec 597 if bx == -3565
sdw inc -637 if vb < -2311
qet dec -871 if obc == 2458
tzy dec -927 if bx <= -3560
xjo inc -471 if w == 442
wl inc 976 if j < 2304
tzy dec -505 if xbh <= -894
a inc 929 if azg == -1586
eh inc -265 if n < -877
w dec -393 if obc < 2469
a inc 406 if xjo > 2324
c inc -437 if j < 2301
a inc -589 if i <= -2399
c inc -101 if obc < 2471
iq inc -478 if iq <= 232
xjo dec 635 if v >= 685
w dec 485 if vjx == -7604
wl dec 490 if eh >= 1571
wl dec 923 if k == -4595
azg inc -237 if bx == -3558
azg dec 820 if vb >= -2322
kxm inc -371 if qet < 2918
i dec 405 if wl != -6507
qun dec -596 if udh > -1178
v dec 973 if wui == 2286
as dec 840 if sdw <= -7121
udh dec 373 if w <= 831
ebu dec 372 if iq > -253
n inc 469 if qet == 2916
udh dec -123 if obc > 2457
wui inc 517 if c != 3270
wl dec -729 if kxm < 578
vb dec 934 if wui >= 2800
qet inc -307 if wui >= 2803
sdw inc -935 if c <= 3266
tzy dec 14 if w < 841
k dec 167 if eh <= 1579
j dec 438 if bx >= -3573
obc dec -452 if azg >= -2415
xjo dec -41 if ne > -3056
qet inc -482 if qet != 2614
obc dec 390 if bx >= -3571
v inc 289 if azg <= -2410
k dec -777 if azg <= -2397
xjo dec 658 if azg <= -2406
c dec -681 if ebu > 2332
n dec 877 if ebu != 2334
ne inc -602 if azg > -2410
w inc -334 if eh == 1574
c dec 64 if sdw < -8051
bx inc -76 if udh > -1043
vjx dec -169 if c >= 3263
as inc 941 if vb != -3248
i dec 131 if xjo < 1084
as dec 222 if n <= -1750
i inc -249 if wui < 2811
obc dec 28 if tzy == 4635
n inc -816 if xjo <= 1087
obc dec 872 if qet >= 2624
vjx inc -201 if ne == -3649
eh dec 602 if iq >= -250
w dec -596 if i != -3188
w dec 212 if vjx < -7436
xbh dec 523 if kxm == 577
n inc 962 if v <= -281
iq dec 30 if c <= 3268
obc dec -300 if sdw == -8050
vjx inc 125 if eh <= 984
sdw inc -470 if j >= 1854
vb inc 295 if udh >= -1045
skh dec -79 if wui > 2795
skh dec 307 if iq <= -272
azg inc -74 if j != 1865
qun inc 745 if as == 3182
i dec -928 if wui < 2809
obc inc 63 if wui >= 2800
xjo inc 114 if ne < -3644
qun dec -92 if as <= 3185
obc inc -363 if w != 1211
tzy inc 194 if ne == -3651
xjo dec 569 if as < 3185
i dec 295 if j >= 1848
ne inc 946 if w == 1219
skh dec -149 if j >= 1866
kxm inc -579 if c != 3271
a dec -979 if iq < -275
xbh dec 281 if k == -3985
xbh inc 88 if obc != 2207
vjx dec 881 if udh <= -1042
i inc 641 if qun < 3117
vb dec -132 if vjx != -8192
udh inc 952 if obc <= 2195
ne dec 562 if iq > -287
xbh inc 673 if wui <= 2800
bx inc -737 if kxm != 8
w inc -766 if wui >= 2798
tzy dec 838 if vjx > -8198
i inc 435 if obc != 2195
a inc -582 if wui > 2796
azg inc -87 if vjx == -8194
udh dec 634 if ne >= -3274
j dec 235 if kxm != -2
v dec 143 if vb < -3114
as inc 70 if bx > -4308
w inc 864 if n < -1616
bx inc 959 if n < -1604
tzy dec 933 if i != -1469
ne dec 369 if ebu > 2320
v inc 387 if qet >= 2621
qun inc 616 if k != -3985
xbh dec 901 if skh != -3276
k dec 770 if bx > -3347
c inc 345 if azg > -2567
sdw inc 485 if udh != -1672
ebu dec 697 if xbh == -2515
qun inc -223 if obc < 2203
vb dec -329 if as == 3250
i dec -828 if qet == 2614
i dec 457 if ebu > 2317
c inc 859 if vb >= -2794
vjx dec -324 if xbh < -2519
tzy dec 350 if sdw >= -8037
wl dec -546 if a <= -4346
ne dec -422 if vb <= -2784
iq dec 933 if udh == -1682
vb dec -354 if w < 461
as inc 222 if ne == -3211
i inc -488 if c == 4123
v inc 466 if bx < -3338
vb dec -779 if iq < -1208
i inc -54 if i > -1581
vb inc -124 if as > 3248
qet inc -25 if i >= -1586
ebu dec -572 if qet == 2589
bx dec 699 if k >= -4763
iq inc -184 if kxm != 2
kxm dec 986 if xbh <= -2515
j inc 904 if vb != -1773
a dec 862 if vb == -1778
wui inc -604 if ne >= -3223
a dec 682 if qun == 2884
qun inc -315 if iq <= -1395
n dec -930 if qet < 2598
n inc -532 if iq <= -1406
w inc -921 if xjo > 618
bx dec 728 if a > -5891
skh dec 463 if sdw > -8038
i inc -119 if c != 4128
n inc 651 if ebu >= 2894
xjo dec 978 if eh != 982
i inc -948 if n == -28
skh inc -459 if v < 41
j inc 625 if kxm == -988
skh inc 38 if k >= -4757
eh inc 207 if eh > 968
k dec 771 if sdw < -8027
vjx dec 73 if qet <= 2592
j dec -53 if iq >= -1403
vb inc 952 if vjx <= -8263
azg dec -345 if as > 3240
w dec -802 if ebu <= 2901
azg dec -861 if iq <= -1394
xbh dec 160 if xbh > -2522
vb dec 170 if wui != 2201
xbh dec -607 if azg <= -1361
obc inc -204 if as >= 3245
n dec 717 if wl == -5769
tzy inc -417 if qet < 2594
qet inc 516 if kxm > -990
tzy dec -355 if azg < -1356
xbh inc 946 if eh >= 1192
iq dec 221 if ebu <= 2904
c inc -692 if vb > -1006
xjo dec -599 if obc < 2004
ne inc -255 if vb >= -1000
vjx inc 144 if sdw != -8038
tzy inc -905 if ne >= -3467
j inc -71 if qet < 3108
as inc 289 if skh == -4165
i dec -621 if azg > -1371
xjo dec 703 if w == 334
wl inc 657 if azg < -1358
n inc 152 if sdw < -8041
skh inc 258 if vjx == -8123
iq inc -613 if w >= 330
azg dec -55 if azg < -1366
w inc -504 if n == -745
vjx inc -100 if kxm < -996
j dec -699 if tzy > 3577
wui inc -120 if i > -2038";

    timely::execute_from_args(std::env::args(), move |worker| {

        let index = worker.index();
        let peers = worker.peers();

        let mut state = ::std::collections::HashMap::new();

        let mut max2 = 0;

        for line in input.split('\n') {

            let instruction = line.split_whitespace().collect::<Vec<_>>();

            let read = *state.get(instruction[4]).unwrap_or(&0);
            let val = instruction[6].parse::<isize>().unwrap();
            let test = match instruction[5] {
                ">" => read > val,
                ">=" => read >= val,
                "==" => read == val,
                "<=" => read <= val,
                "<" => read < val,
                "!=" => read != val,
                _ => panic!("unrecognized instruction"),
            };

            if test {
                let change = match instruction[1] {
                    "inc" => instruction[2].parse::<isize>().unwrap(),
                    "dec" => -instruction[2].parse::<isize>().unwrap(),
                    _ => panic!("unrecognized action"),
                };
                *state.entry(instruction[0].to_string()).or_insert(0) += change;

                if *state.entry(instruction[0].to_string()).or_insert(0) > max2 {
                    max2 = *state.entry(instruction[0].to_string()).or_insert(0);
                }
            }
        }

        println!("part1: {:?}", state.iter().map(|(_k,v)| v).max().unwrap_or(&0));
        println!("part2: {:?}", max2);

        let worker_input =
        input
            .split('\n')
            .enumerate()
            .filter(|&(pos,_)| pos % peers == index)
            .map(|(pos, line)| (pos, line.to_string()))
            .collect::<Vec<_>>();

        worker.dataflow::<(),_,_>(|scope| {

            let data = scope.new_collection_from(worker_input).1;

            let edits = data.map(|(pos, line)| {

                // Each line has a read location with a condition, and a write location with an operation.
                // E.g., 
                //
                //      kxm dec 986 if xbh <= -2515
                //      obc inc 63 if wui >= 2800
                //      xjo dec -599 if obc < 2004
                //      ...

                let mut words = line.split_whitespace();

                // Extract the write location and what we should add.
                let dst_name = words.next().unwrap().to_string();
                let dst_flip = words.next() != Some("inc");
                let mut dst_diff = words.next().unwrap().parse::<isize>().unwrap();
                if dst_flip { dst_diff = -dst_diff; }

                words.next();   // Ignore the "if".

                // Extract read location, comparison, and value.
                let src_name = words.next().unwrap().to_string();
                let src_cmp = words.next().unwrap().to_string();
                let src_val = words.next().unwrap().parse::<isize>().unwrap();

                // (pos, (src_name, src_cmp, src_val), (dest_name, dest_diff))
                ((pos, src_name), ((src_cmp, src_val), (dst_name, dst_diff)))
            });

            // Our plan is to iteratively improve a set of transactions to commit, starting from none.
            // In each iteration, we need to determine the value of each read location at the position
            // associated with the transaction, which we will do with a prefix sum. Then, based on the
            // value we get back, we (tentatively) accept transactions and proceed to the next round.

            // let reads = edits.map(|(pos, (src_name, _, _), _)| (pos, src_name));

            let history =
            edits
                .filter(|_| false)
                .map(|_| ((0, String::new()), 0))
                .iterate(|valid| {

                    let edits = edits.enter(&valid.scope());

                    valid
                        .prefix_sum_at(edits.map(|(key,_)| key), 0, |_k,x,y| *x + *y)
                        .join(&edits)
                        .filter(|&(_, sum, ((ref src_cmp, src_val), _))| match src_cmp.as_str() {
                            ">"  => sum > src_val,
                            ">=" => sum >= src_val,
                            "==" => sum == src_val,
                            "<=" => sum <= src_val,
                            "<"  => sum < src_val,
                            "!=" => sum != src_val,
                            _ => panic!("unrecognized instruction"),
                        })
                        .map(|((pos, _), _, (_, (dst_name, dst_diff)))| ((pos, dst_name), dst_diff))
                        .map(|x : ((usize, String), isize)| x)
                        .inspect_batch(|t,_xs| println!("{:?}", t))

                })
                .consolidate();

            // part1: Maximum value at the end.
            history
                .explode(|((_pos, dst_name), dst_diff)| Some((dst_name, dst_diff)))
                .count()
                .map(|(_key, cnt)| ((), cnt))
                .group(|_, input, output| output.push((*input[input.len()-1].0, 1)))
                .consolidate()
                .inspect(|x| println!("part1: {:?}", (x.0).1));

            // part2: Maximum value at any point.
            history
                .prefix_sum(0, |_k,x,y| *x + *y)
                .map(|(_, sum)| ((), sum))
                .group(|_, input, output| output.push((*input[input.len()-1].0, 1)))
                .consolidate()
                .inspect(|x| println!("part2: {:?}", (x.0).1));

        });

    }).unwrap();
}
