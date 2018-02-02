extern crate timely;
extern crate differential_dataflow;

// taken from: https://adventofcode.com/2017/day/6

use differential_dataflow::Collection;
use differential_dataflow::input::Input;
use differential_dataflow::operators::*;

fn main() {

    let input = "czlmv (78)
fiwbd (57)
twxnswy (98)
dhrnu (62)
yptrqp (64)
vikplll (9)
lgxjcy (359) -> vsbfmt, kbdcl
tvisu (70)
imxraee (33)
qfbqiv (55) -> gwlyvbx, uanby
eolilw (78)
gywnt (52)
dnybu (88)
yiyhuxk (15)
axrep (33)
ajtjym (11)
zjmhab (68)
xcstcy (20) -> iequdmw, fozfiqf, ytkmda, ywawq, nfgmyd, bzumy
dgscbm (29)
sghuxq (9)
jfmuzo (164) -> istoj, jyzrmnp
zjklq (52)
zwcucws (104) -> vqghe, alycm
ldfhqjw (809) -> wpprys, heotf, dybitt
ckbjuyy (63) -> ygvwdmg, pwxorqj, hshbiaq, oxqvzzv
xnalv (11)
owkily (191) -> cqnmo, fhqah
wrbknnc (80)
imakkq (99)
iymrfq (57) -> iwdjjf, zamje, eduxyw
umzsfe (66)
ruvmsg (55)
ewtdu (66)
uwcqi (91) -> umzsfe, tqjzv
nqqxz (123) -> jrkbh, hyyeb, abwmmuw
metyk (77) -> euriiux, zztvgwh
smxwnsk (16)
xhodzi (31)
unscz (67) -> dpavyu, lrdmltp
yrluhjr (50) -> qdpcslx, echdbt, aeeas, uodyv
vvqtv (91)
sigxauh (64) -> dnybu, kpnbj
nqjmi (49)
rsocykv (55)
vqghe (93)
lumomlj (5)
ewods (83)
qggycuv (26)
zqbtgtw (94) -> ewtdu, xpmpulq, kpnjmwz
qnbnflf (53)
aintn (73)
ibnho (16)
vsbfmt (25)
zwake (64)
swpvgrn (36)
vlsyqg (93) -> lrauqzk, gdhekde
zawbrvg (10)
wqgczyl (200) -> ilgue, bwdfntd, zlgst
qhyan (391) -> obxyz, ahutsc
vrppl (78)
rqebmc (176) -> snnxhgn, anucwoj
hhpib (98)
iqdjcwm (57)
xpmpulq (66)
afckhxn (16)
uynmvcu (304) -> imxraee, rmfwij
cybhcko (256) -> pgfomf, dacrm
ylrxgoz (35)
edmzea (60)
xuokqm (35)
rmfwij (33)
gtezger (64)
dmjech (59)
avbnxz (47)
aapssr (72) -> twimhx, wfdiqkg, guuri, qwada, mwsivlf
guuri (38) -> ydmxb, lndfaxo, sjfue, hufrsq, rxfma
uyaarr (55)
jtudcya (98)
fwgdynj (92)
kpnbj (88)
anucwoj (13)
paxdij (10)
cadrzx (29)
odbbac (614) -> lnkowzc, zqcluqc, pvatk, thymhif, gchcumo, rffqotx, juscmde
maiiqc (133) -> apagba, mhhbej
voeps (50)
apagba (36)
drpgz (590) -> zabqp, vercm, maiiqc, egyfwlq
hgtyjhv (23) -> ffqwo, yqzdjj, yrwcj, ilxgo, bvoxahz, iymrfq
zjevsui (23) -> feiitan, upgfr
btzlwo (37)
vzpqvfx (51)
liwqqqi (117) -> ckdeo, movesh, abgfgav, mrlnt
udslj (75)
twhcdv (58) -> uqzoo, ytwvl, gjxgd, ehcrq
qtfzj (92)
xlufcc (1391) -> qlsdtum, swkor, ndsvlr
hghpgj (11)
oxckpuw (218) -> nssexu, uwogu
rlwyzv (86)
wontek (77) -> lqfsw, ejljdxo, jtudcya
zpeuqlo (57)
coibvjw (65)
xuqozk (9)
wdyxwia (45)
zetdtq (93)
enymia (66) -> ckurzc, lcohspm
heotf (5) -> gminpu, jodhx
bttlx (37)
snnxhgn (13)
uqcxvh (70)
zvxhi (92)
yfzypu (50)
xhrttm (17)
erhiag (13)
wbnux (92)
upgfr (87)
hxkeypq (43)
dexabuc (5)
cjjwz (27)
kbdcl (25)
rrmtlri (80) -> jkknc, vvkrst, vblyk, ghiftw, tilvzsd, drzoi
ujaiypo (31)
mgcyq (91)
mqrgut (255) -> dhfrnu, bzijz
zobnnps (411)
cxjmggj (286) -> kmkqsvl, remwhfn
xpinqek (50)
fzvyv (14)
vvkrst (264)
lrdmltp (64)
mjgilv (163) -> ifegka, olwcj, eheeelo, cvjhub
rhqfa (80)
wswjfu (52)
tqbouv (443) -> dtqxiic, chsuzvr
xxyjm (267)
jofbj (256) -> vikplll, vjaqo
zbtptb (394) -> hiuuk, diriz, liudq
ixuyx (92)
yfkgike (56) -> dhrnu, ehzvm
rchhwx (46)
gatdc (35)
zheomzg (12) -> kkimdb, hhomxd
lzstd (47)
zqzrxe (10)
kpnjmwz (66)
qvpob (322) -> vtzfsci, uexfxv
ckurzc (85)
rpxhqmz (240)
mhhyoxf (86)
ryiytg (63)
kmxek (226) -> rbgonm, qgfxzo, sghuxq, hauul
idprbpo (146) -> wkurl, plwdhth, staoh
liudq (19)
cmouh (82) -> hmulqgo, qjsjrz, owkily, jkqbf, bipzxj
pneftv (97)
ulwzhy (54)
vbdycf (931) -> uwrmetx, lruhvsn, hssfwi
gdckdzx (37)
dueof (87)
hooxvp (40) -> rahsrp, uqcxvh
nxgnumb (91) -> hdmdxi, oxxse, jhmvp
vekdbjs (15) -> etznlt, kvwqrd, udslj
ptamwey (245) -> laovxxw, cpdtw
ictgyp (241) -> zbiypv, rrmtlri, odbbac, tkqke
dybitt (37) -> ulwzhy, wrumij
cfuned (17)
nvgnls (54)
iwdjjf (79)
fvifqfr (91) -> uxgbnjk, phckimp, apeba, udzzv
cbtgtqb (24)
yidnjq (10)
bbdayq (86)
bzzsss (92)
zedqbr (26)
yljjx (276) -> wumlwia, rnvlepv
sarwcau (48)
rquaisv (389) -> fbbkkxh, xhodzi
zlgst (54)
dxxzpr (16)
vatlgyb (16)
qojqcd (5)
azpmzli (262) -> xodsc, tzlzl
husns (217) -> btzlwo, sntovuq
wltlu (59)
nzzwzmh (38)
apeba (90)
cflrkd (31)
fbmalu (275) -> qtqbdp, ldrfyi
qnvby (57)
ovgpa (29)
weejuk (64)
ourium (26)
yckuqt (43)
hyyeb (53)
hgrwjoy (221)
cfqczei (39)
bhwnch (184) -> qhyan, fvifqfr, rquaisv
iibalc (73) -> dgmevpb, mojye, zcfztz, qsvyfv, ulxxt, hzxiv, spovj
tlpydjm (48)
pfvoay (31)
vmpev (37)
eheeelo (22)
tzlzl (6)
oncjf (68)
kirowa (37)
uqzqmo (124) -> pgkeahi, owgile
pwxorqj (91)
dhfrnu (66)
ogjqrb (199)
mwsivlf (18042) -> xcstcy, jluccf, pftdcqs
cqbtoo (11)
hlysw (14)
ekgsba (57)
zcfztz (228) -> scyuq, vggwo
owgile (18)
keczktv (10)
gtixbu (85)
hgywlzc (135) -> osysik, fmmgboi
bwdfntd (54)
atoveeu (297) -> crtnt, qnvby
muecmt (51)
tqjzv (66)
zsraq (47)
idoml (79)
mwgimvy (49)
gchcumo (138) -> msqtvwu, feqefvb
zrmqrif (32)
qsrfkq (118) -> sgxil, tiugk
lrauqzk (88)
dckozkt (89) -> kbefh, pnqri, zdnvz, tuzquw
evsnlxn (99)
vwwvjg (1005) -> nqqxz, dfewulo, qsrfkq
dacrm (12)
wnrqkg (362)
nimeytn (147) -> difavgr, idcvpt, mjgrhot, glllgqs, qsguy
bdrerk (146) -> oswqr, gjjyumz
zfdhua (65) -> deewhp, nerzb
kfjbjl (243) -> hatmf, tywedfu
vsijk (213) -> hvtsr, qwhelvj
lcohspm (85)
brqvmh (70)
sinhju (87)
uilshuf (20)
jodhx (70)
puiks (20)
wvuqq (65)
zamje (79)
rqfjzox (86)
yjfazsr (44)
kufsr (425) -> sdeana, byrxbgo, tvarspr
nbmat (39)
gwrred (131) -> aujwg, touzm
svkcvg (53) -> rbzqh, abnhpi, zrmqrif, kvtpc
dasbhc (80) -> pmlim, gwoiid
opnbls (202)
knqqz (123) -> vebli, axwdrai
jyzrmnp (49)
difavgr (236) -> ukzfd, rxkpkvi
wpjaa (76)
sbfhxsx (99) -> pusdii, qqodh, tlpydjm, jgoaja
qaedcl (61)
oxwnu (24)
gtvpf (65) -> knqqz, qzfwf, vlsyqg, anvxtbs, idprbpo
huansqc (29)
oaabog (705) -> nmbql, yodnj, yrluhjr
ghiftw (182) -> gpikz, mvkxva
qfhfg (50)
zqcluqc (30) -> kdwzx, yflpz, aokvbbi
twimhx (25773) -> xsmdo, gpormp, akioek
brdbeoe (23)
cfshobe (91)
jojnh (241)
ettsd (32) -> rzpmulz, fmjfhoo
liymh (86)
hmulqgo (275) -> xjrheul, cykhmjj
zbptsb (80)
vjaqo (9)
wmfro (410) -> mcpyfmy, rzlxx
wpprys (113) -> tkcmrl, dxxzpr
farpozz (13) -> wepvw, zzjmukx, ooenol
wpwxn (51)
izphu (97)
epuiwg (54)
efcqep (79)
dnvqlog (53)
knwlsi (54)
zfrsmm (763) -> aapsp, gfsnb, jojnh
zjxcrv (43)
lnkowzc (88) -> aaqsdv, ujaiypo
vgtxty (199) -> fcbxsf, acchih
grgjs (345) -> uvntywl, nzzwzmh
lewfa (45) -> aouljs, swvae, ybwgz, spdsz
nfgmyd (1390) -> yljjx, lcdleci, zcoyhde
zzjmukx (69)
vjwptwp (36)
spfds (86)
vqbsvk (72)
eyslsq (84) -> oncjf, nuibgd
gvegx (90) -> qhujoiu, dpvmn, cyhnayc
opfey (37)
ypiwj (27) -> pupaehu, hekhbd, kgfldlb, atoveeu, zobnnps, iukfd
vzkneq (52) -> gvegx, mqrgut, bsvmale, fjueoko, cdoed
kynrf (104) -> bnmgqen, iuftirx
wibgm (83) -> zetdtq, dcytmqp, xfxqvk
alycm (93)
rcnhfcz (13)
smbkvwd (276) -> rvwvqij, avbnxz
kitggwr (91)
kbsmiq (70)
touzm (32)
xyfeczu (317)
slnezfv (83)
xpizz (60)
seicwz (964) -> izpxbq, gdomj, jrsbjr
mbjsjy (51)
aoltxw (51)
zztvgwh (87)
rznqdxq (60)
vggwo (13)
tlxdyxj (1022) -> cnztmm, dkjwj, drigjl
qjsjrz (125) -> aqrxpru, polnw
khpntc (92) -> hxkeypq, wfefbdt, mhbov
qrdtzc (35)
glllgqs (348) -> paxdij, yidnjq
jnghyrm (107) -> soebcx, opfey
abnhpi (32)
tziqot (226) -> peyyn, chwoyn
hgdbip (10)
goqhcu (60)
kddex (87) -> goqhcu, rxohrzb, ucsvcy
zfzwko (20)
uhjxtrp (16)
zmzwttu (54)
vdyftz (50) -> ypttuns, gfcfsf, ymonx, fjdyms
ctien (177) -> cqbtoo, hghpgj
jfqlxa (86)
nhwgy (44)
lruhvsn (86) -> ljeiizi, opctg
uxvgnv (29)
cwdoo (164) -> eozev, gohdd
ghkmb (165) -> pnuvuf, nskayc
wumlwia (34)
xryqzl (52)
fbhnbdw (90)
wuijd (50)
dannmef (61)
aokgycq (50)
wfefbdt (43)
zmqxmtf (92)
biioe (70)
wwcqz (55)
echdbt (83)
tdtxcm (35)
tvarspr (6)
zkhtua (187) -> zmbimk, kmxek, tziqot, aewbhax
phckimp (90)
rcomsru (73)
aaztou (24)
pwdoio (184) -> nxcus, bpbmbq, zbtptb
impqm (64) -> yumbba, zltzea
bkjcc (41)
cnztmm (255)
jdbel (87)
vcjavrw (60)
udkba (87)
jretzi (202)
donza (96)
staoh (41)
subnd (77)
jzrlfrl (86)
sgxil (82)
wahmvp (49)
lvtxk (35)
cxrukf (5)
atcole (135) -> ykwvu, yckuqt
bjeto (71) -> msoip, wjkky
qsvyfv (220) -> cfuned, vzayzaq
ztodsnq (344) -> lqrde, ukvzqny, svzqi
pkditpt (99)
rgshogg (33)
drefe (29)
rnqvaa (91) -> wvuqq, uifug
cpxkqg (96)
bstznzu (45)
iflhr (7)
pczpj (60)
jhmvp (30)
aouljs (68)
ndqomrl (368) -> fhyfhjg, qnbnflf
wrumij (54)
ehcrq (299) -> cpllg, asnsvv
auvkb (40)
gdomj (191)
dnnlgt (77)
wasnz (63)
rzpmulz (85)
tiugk (82)
sdjzc (27)
jdxth (14)
aeeas (83)
crtnt (57)
ytwvl (313) -> nqjmi, mwgimvy
avmhq (86)
juscmde (150)
kbpvvf (72)
wegmegy (280) -> oaantta, fzvyv
kfdtg (68)
matvb (55)
ehzvm (62)
ebxyi (54)
qgfxzo (9)
zoodui (220)
dapjhx (93)
ywawq (1093) -> ptcby, kufsr, pxsuagy
laovxxw (27)
furny (8) -> lgxjcy, hcsiqp, vsijk
gzocll (70)
kifypgu (63)
kywpr (91)
eikjv (10)
wjkky (57)
fkwsxxk (1446) -> eyixz, bjeto, ghkmb
weeriq (19)
rxkpkvi (66)
bipzxj (81) -> bpgafw, ilidonx, uxswi
trjird (51)
istoj (49)
idcvpt (71) -> zzdgmmd, imakkq, jbodnl
pehio (202) -> bstznzu, xzdhzoa
exxbgm (77)
tuzquw (83)
ydmxb (1610) -> udoiqj, isfyuxu, hgtyjhv, tlxdyxj, inreau, qiinez
jmsuq (31)
dkjwj (155) -> nbbiqh, nbfbkao
xxbwj (122) -> xpinqek, xqmsvth
dfewulo (48) -> phmmydn, eyjdqm, qpblhez
ukvzqny (47)
gpormp (3465) -> ymogkwe, ywtwrq, hixtw, zmssxbl, twhcdv
diriz (19)
zltzea (47)
jkqbf (87) -> wpwxn, vzpqvfx, muecmt, trjird
uodyv (83)
fhqah (50)
aapsp (5) -> nvhbnlc, dzqax, ndjjag, dmjech
bxfwhrd (51)
knoziw (29) -> sinhju, otbxf, udkba
wbupk (63)
ixvxtb (87)
rnvlepv (34)
nerzb (81)
brtmsqe (59)
gjjyumz (81)
ogpvd (53)
ypttuns (56)
yzkpvn (50)
hssfwi (102) -> qfhfg, uvkecqz
ooenol (69)
mucpe (31)
bpbmbq (83) -> irkhv, navnyn, bzzsss, wbnux
kkimdb (84)
sfgatoz (40)
bqfor (66)
tobsrs (89)
polnw (83)
xlqycb (20)
uwrmetx (202)
jtjzsr (13)
tehdy (16)
feiitan (87)
hcsiqp (65) -> rlwyzv, liymh, huzjje, spfds
udghm (35)
iequdmw (572) -> cwxgg, geqnwz, smbkvwd, swhnbvw, uynmvcu
mwxiwty (90) -> lvtxk, ylrxgoz
xqmsvth (50)
ulszh (64)
cpllg (56)
oswpicl (51)
gcglasa (143) -> nbmat, cfqczei
kdwzx (40)
igumiu (373) -> ggynw, lrodp
jptqm (200) -> avrfgx, mucpe
yrwcj (112) -> cfshobe, mgcyq
uvkecqz (50)
mjgrhot (314) -> sdjzc, cjjwz
ohuvi (89)
uplxy (60)
ymfurx (26)
vrkrr (55)
tkxuj (86)
mydixq (60)
obgin (293) -> lewfa, ukwydlj, xyfeczu
swvae (68)
adlvipk (168) -> uilshuf, qxkilb
vxjhric (220)
mhbeqm (99)
vcyfwm (88) -> qrdtzc, gatdc
xujof (5)
rjkhl (257) -> mogla, nlewmv, mxxecyh
oxqvzzv (91)
rwxmoi (265) -> dnvqlog, ogpvd
tcybjv (178) -> iuneyv, cgszcig
xtpgpbx (15)
umtpolq (98)
zkdwxd (64)
gqiiycg (97)
pftdcqs (2087) -> xeefzwg, vyudgjy, ylvvgmj, fefnhby, ypiwj
vyudgjy (1079) -> ettsd, knastam, jretzi, rqebmc, kngspv, onudby, opnbls
micfuva (85)
dgmevpb (120) -> xcmre, upslw
ncghncy (36)
oxtckvl (144) -> erhiag, kvcak
tilvzsd (98) -> slnezfv, sxick
snaebf (11)
sdnzxl (2345) -> xhkrx, tcmkx
qdpcslx (83)
ymogkwe (597) -> srqbs, hgrwjoy, khpntc, atcole, spdyx
tywedfu (92)
chhmaxr (10)
onudby (18) -> zvxhi, zmqxmtf
xdwzfh (45)
avrfgx (31)
ctafsp (26)
cnbvz (360) -> zpeuqlo, iqdjcwm
uuijd (98)
gwlyvbx (86)
aewbhax (70) -> glhbvi, ulszh, rbawcc
fcbxsf (15)
yqzxa (306)
bbmbn (65)
nskayc (10)
ywuxmil (54) -> wasnz, ryiytg, raaxwq, jtkrr
dtrxzld (131) -> wontek, rjkhl, rwxmoi
shetkt (9) -> jqmcro, rqfjzox
movesh (26)
uylnr (46)
uexfxv (43)
tjxms (92) -> cfwox, vpcfu, boiuyt, nvgnls
cyhnayc (99)
rwtdhr (49)
glxtiz (80)
hhmrwj (178) -> oswpicl, bxfwhrd
nwkxd (19)
dxgntli (46)
xodsc (6)
ffgplgk (117) -> bkjcc, bclkir, cwhjt
fdcnhr (92)
waojp (14)
jtkrr (63)
mhbov (43)
sxick (83)
qwhelvj (98)
thrmrw (134) -> tliov, xhpvb, vjbjy
navnyn (92)
rpjiwuz (85) -> jmsuq, ctqgj
cfwox (54)
ghwwfca (131) -> fsmidq, aaztou, bpcrtwl, vfojca
pnqri (83)
lqdqct (98)
etznlt (75)
geqnwz (370)
orydf (91)
ywtwrq (1021) -> qfbqiv, ghwwfca, zfdhua
ayrucb (14)
jkmernz (94)
rxohrzb (60)
rffqotx (84) -> rgshogg, axrep
xxtdti (83)
jluccf (8332) -> xlpzp, obgin, dtrxzld, nzbknfm, ldfhqjw
pnuvuf (10)
iqqevm (80)
iuftirx (33)
sprno (14)
zcyfth (24)
poenou (76) -> evsnlxn, mhbeqm
pxifep (91)
tlgew (49)
xriiu (15)
hxzthym (303) -> zsbcn, nwejb, dckozkt, grgjs
jagoxt (10)
nuibgd (68)
emqqx (17)
erzayz (611) -> ywuxmil, clhyd, yqzxa, cxjmggj, kcqvnd, cvipqzv
umjwkfc (94) -> wpcry, pkbxhn, xmnreh
tjnxnd (123) -> gnitpo, yfzypu
kxftjs (43) -> tjxkxom, hrbsamg, qvpob
dbrlat (411) -> sprno, waojp
dqggz (76)
aqrxpru (83)
qedsq (894) -> sscfkq, waxrcby, fkwsxxk
kngspv (60) -> hhuhid, mhoaev
yonxye (177) -> qallt, vmpev
goqdmgy (85) -> fiwbd, ekgsba
nboczvq (55)
zlrvs (78)
zabqp (147) -> uxvgnv, huansqc
ndsvlr (88) -> mbjsjy, mjxtbvx
hnnjspe (79) -> wuijd, aokgycq, yzkpvn
hjjri (1300) -> vrkrr, jaxae
bvoxahz (184) -> kmvkkze, nboczvq
bpareqw (391) -> pehio, pxjve, zqbtgtw
afpxssv (45)
ibzpdf (119) -> gywnt, jogksw
sdeana (6)
sgzijsv (76)
peyyn (18)
xdszea (185) -> kbpvvf, vqbsvk
kvwqrd (75)
tyuyffp (228) -> tlcseu, xryqzl, hraby
fistzz (1760) -> vgtxty, bffvo, hnnjspe
zqqyhq (60)
qwada (40305) -> wfkcsb, qlboef, pkowhq
remwhfn (10)
vatqulm (66)
psirxj (42) -> rcomsru, aintn
mhblqw (10)
uwogu (38)
fsmidq (24)
bsvmale (79) -> exxbgm, trvjrbi, subnd, dnnlgt
zsbcn (421)
ilgue (54)
ldsbhlt (153) -> ayrucb, hlysw
scyuq (13)
piqgu (833) -> dkwde, dkhjdb, dqoatul, psirxj, mbmaqh, gxdqpcd
qsemjfo (241) -> yjfazsr, aeiktt
bfxmh (94)
rrnjpdu (280) -> lmuwznh, iflhr
jaxae (55)
nbbiqh (50)
vwisj (44)
gwoiid (78)
ctidma (26) -> ncghncy, swpvgrn, xwtgr, vjwptwp
oovlaqz (69)
fezhnw (71) -> ixuyx, pkesm, fdcnhr, qtfzj
hauul (9)
dkhjdb (66) -> dannmef, qaedcl
rtlde (120) -> glxtiz, rhqfa
qtfuwru (24)
mqyipy (23)
hvrmmxf (94)
swhnbvw (54) -> zodbq, qmiurj, idoml, efcqep
zcoyhde (92) -> esjuqh, vxucz, kifypgu, bveaw
qhujoiu (99)
sophzzn (407) -> uhjxtrp, vatlgyb
wfhixl (15)
eyixz (175) -> hmeolu, lumomlj
ldrzzzz (11)
hdmdxi (30)
azrcn (98)
jbodnl (99)
dkrdqpo (15)
wpcry (62)
ivnufgs (93)
mgreg (68)
vxobef (73)
nxcus (361) -> wdyxwia, nhhyvm
bdgwdt (280)
mculw (54)
jmavfy (35)
asnsvv (56)
wpezq (224) -> qppamj, pokjgkl
gsaci (313) -> tkxuj, jzrlfrl
eeuohnm (70)
zzdgmmd (99)
msoip (57)
spovj (58) -> lqdqct, omxny
covpz (123) -> wbzwxs, hvlfx
ukzfd (66)
ygldlhw (17) -> bbmbn, coibvjw
xmnreh (62)
nssexu (38)
zibnjxk (11)
fhyfhjg (53)
jaebowl (50)
vfojca (24)
gminpu (70)
ylpahav (29)
zdnvz (83)
tlulph (94)
hvlfx (49)
sadhkpb (204) -> edmzea, pczpj, jxevrgz
thymhif (150)
ewfql (37)
cykhmjj (8)
xlpzp (158) -> jnghyrm, nxgnumb, ldsbhlt, svkcvg, avnosfl, shetkt
ylvvgmj (1785) -> csvxlo, enymia, dasbhc
tdfgfqn (55)
rnyarzh (44)
exgsu (94)
heqvbi (48)
ybwgz (68)
rbzqh (32)
iwwngux (98) -> hvrmmxf, nebwj, bfxmh, fkrdkav
wolyko (13)
jrsbjr (45) -> vxobef, zsjoz
wowgkro (1847) -> vcvjtf, biioe
xjrheul (8)
gohdd (63)
tlfbyq (168) -> zqzrxe, jagoxt, zawbrvg, chhmaxr
jgoaja (48)
ghmxos (43)
bvqcnc (195)
ffqwo (116) -> ohuvi, ygtbl
qallt (37)
aujwg (32)
upslw (67)
axwdrai (73)
hyeetiq (70)
inreau (23) -> rosop, baimp, oxckpuw, nehwool, rrnjpdu, rqecouz
wfdiqkg (27213) -> ictgyp, yffxaiy, prvckio, qedsq, fjmjc
obxyz (30)
hatmf (92)
xffmho (423) -> xdszea, qsemjfo, fvfehj
omxny (98)
kfjqheo (1683) -> gwrred, bvqcnc, unscz
bffvo (169) -> puiks, zfzwko, xlqycb
tpbeg (6) -> sgzijsv, ysgnao
onwqp (22) -> mowkbi, gsaci, wuogj, ztodsnq, tqbouv
fcfbxmb (73)
iuneyv (48)
qybyrm (98)
cszchsg (73) -> ieqgh, fezhnw, sophzzn, dbrlat, igumiu
lrodp (33)
qmiurj (79)
rbawcc (64)
hhomxd (84)
euriiux (87)
esjuqh (63)
pxsuagy (369) -> ewfql, gdckdzx
xhpvb (52)
jroea (42) -> jmavfy, udghm, tdtxcm
tmfyif (15)
stfqd (59) -> zqqyhq, uplxy, rznqdxq, xpizz
vcwlaw (68) -> zeaxycf, izphu
eyrgmc (102) -> bbdayq, avmhq
pusdii (48)
cgszcig (48)
byrxbgo (6)
ukwydlj (135) -> vvqtv, ffjie
boiuyt (54)
mnbkaa (93) -> pneftv, gqiiycg, hrqaph
vxucz (63)
ejljdxo (98)
dkwde (50) -> shwsv, oovlaqz
aktswyh (51)
dvvkzv (40)
qppamj (8)
bftoy (221)
dzqax (59)
rlrnjnz (1115) -> dqggz, wpjaa
qzfwf (247) -> ldrzzzz, cxfqupv
ffjie (91)
mowkbi (93) -> uuijd, umtpolq, twxnswy, ivowf
ljeiizi (58)
emsjxz (47) -> jaebowl, vyxgc, voeps
hrqgfhg (415) -> vcwlaw, dsrostg, tfzsp, jptqm, vmuqpj, jfmuzo
xwtgr (36)
ctqgj (31)
bwqxk (220) -> xtpgpbx, dkrdqpo, wfhixl, yiyhuxk
kpwfqfc (93)
ulxxt (236) -> xuqozk, tyxfxu
yumbba (47)
rvwvqij (47)
prvckio (93) -> zqjqic, kfjqheo, cszchsg
uifug (65)
zeaxycf (97)
cxfqupv (11)
kcqvnd (306)
pxjve (246) -> rizbeea, brdbeoe
mhoaev (71)
rzlxx (32)
rnoaimc (99)
olwcj (22)
fmmgboi (31)
bclkir (41)
hraby (52)
jqmcro (86)
kvcak (13)
msqtvwu (6)
trvjrbi (77)
jogksw (52)
cdoed (240) -> tlgew, rwtdhr, wahmvp
qpblhez (78)
rqecouz (260) -> emqqx, xhrttm
zmbimk (22) -> zbptsb, wrbknnc, iqqevm
zmssxbl (1033) -> tjnxnd, ibzpdf, uwcqi
qlsdtum (158) -> ibnho, smxwnsk
hyzzwx (93)
bhvvs (173) -> zcyfth, oxwnu
soebcx (37)
lgltgc (98)
lcbljqb (226) -> bcpbir, zlubkqa
jrkbh (53)
icziaym (1507) -> uqzqmo, mwxiwty, ihskp
lqrde (47)
hrqaph (97)
qyrhb (486) -> wegmegy, tjxms, bdrerk
zwvjv (89)
rizbeea (23)
mjxtbvx (51)
drfeinj (30) -> cpxkqg, donza
spdyx (189) -> tehdy, afckhxn
dqxwazb (54)
vblyk (160) -> zjklq, wswjfu
mcpyfmy (32)
xzormv (5)
cwhjt (41)
drigjl (6) -> xxtdti, ewods, dkysxdp
fmjfhoo (85)
tliov (52)
gjxgd (371) -> hgdbip, eikjv, keczktv, mhblqw
cpdtw (27)
xaxuyg (372) -> bvfdo, aoltxw
dpavyu (64)
egyfwlq (133) -> cbtgtqb, qtfuwru, mnjirpy
llrntn (106) -> rcnhfcz, nqzvz, wolyko, jtjzsr
fjdyms (56)
sjfue (11700) -> vcyfwm, impqm, llrntn, tpbeg
nmbql (250) -> bqfor, vatqulm
udzzv (90)
dsrostg (232) -> xriiu, tmfyif
ihskp (42) -> wltlu, brtmsqe
zsxjxs (23)
lqfsw (98)
qsguy (252) -> drefe, ylpahav, dgscbm, emznzp
ilidonx (70)
hekhbd (411)
zodbq (79)
fjueoko (105) -> jkmernz, exgsu, tlulph
idqaieo (88) -> sfgatoz, dvvkzv, auvkb
udoiqj (914) -> sbfhxsx, fbmalu, husns
fbbkkxh (31)
nzbknfm (84) -> thrmrw, cwdoo, zwcucws, knoziw
hixtw (582) -> rtlde, hhmrwj, lcbljqb, umjwkfc
ifegka (22)
bnmgqen (33)
yodnj (320) -> pfvoay, cflrkd
hiuuk (19)
mlafk (38) -> wqgczyl, wnrqkg, wibgm, tpnvx
drzoi (68) -> lgltgc, cpess
dqoatul (42) -> fcfbxmb, qququw
bibly (37)
opctg (58)
abwmmuw (53)
ucsvcy (60)
anvxtbs (97) -> jfqlxa, mhhyoxf
hwevm (11)
qiinez (107) -> wpezq, sigxauh, rpxhqmz, xxlwrd, ffgplgk, hvsywx, vekdbjs
rhnota (37)
mrlnt (26)
ytkmda (1825) -> ctien, ogjqrb, goqdmgy
uanby (86)
oxxse (30)
jkknc (44) -> uyaarr, wwcqz, ruvmsg, tdfgfqn
xxxxbnh (299)
fefnhby (1167) -> liwqqqi, bftoy, covpz, bhvvs, gcglasa, rnqvaa
xsqvwa (274)
xmgank (374) -> dexabuc, xzormv
xxlwrd (194) -> zsxjxs, mqyipy
yyyel (68)
fvfehj (49) -> tvisu, hyeetiq, kbsmiq, brqvmh
ivowf (98)
isfyuxu (1346) -> ygldlhw, jroea, rpjiwuz
cqnmo (50)
mxxecyh (38)
aaqsdv (31)
dkysxdp (83)
chsuzvr (21)
vjbjy (52)
pyzwl (90)
izpxbq (7) -> wennxvo, fwgdynj
fkrdkav (94)
kmkqsvl (10)
oswqr (81)
pvatk (18) -> nhwgy, rnyarzh, vwisj
gxdqpcd (188)
ggynw (33)
acchih (15)
ldrfyi (8)
tjxkxom (230) -> zwvjv, tobsrs
vyxgc (50)
wfkcsb (6251) -> vxjhric, farpozz, eyslsq, zoodui
otbxf (87)
shwsv (69)
uxswi (70)
cvjhub (22)
zkyiauj (41) -> pqnkr, mnbkaa, xmgank, sadhkpb, tyuyffp
akioek (53) -> wowgkro, icziaym, nimeytn, hrqgfhg, hxzthym, vzkneq
emznzp (29)
hvtsr (98)
yqzdjj (33) -> dueof, jdbel, ixvxtb
fqkbscn (418) -> kddex, nylhp, xxyjm, agkgi
plwdhth (41)
abgfgav (26)
wbzwxs (49)
tlcseu (52)
vercm (79) -> aejbixg, wbupk
uvntywl (38)
hmeolu (5)
swkor (80) -> matvb, rsocykv
deewhp (81)
bveaw (63)
qlboef (81) -> gtvpf, qyrhb, xffmho, drpgz, hjjri
cvipqzv (296) -> xujof, vsffvp
mvkxva (41)
osysik (31)
tkcmrl (16)
xfxqvk (93)
sscfkq (720) -> ckbjuyy, zdlxv, kfjbjl
aejbixg (63)
lndfaxo (97) -> rqsjqrg, onwqp, sdnzxl, erzayz, fistzz
bcpbir (27)
feqefvb (6)
xsmdo (4571) -> vwwvjg, iibalc, ysgotr, oaabog
knastam (180) -> zibnjxk, snaebf
waxrcby (83) -> tcybjv, xsqvwa, poenou, eyrgmc, azpmzli, vdyftz, jofbj
bvfdo (51)
nvhbnlc (59)
vcvjtf (70)
mojye (244) -> qojqcd, cxrukf
qqodh (48)
ahutsc (30)
vtzfsci (43)
wennxvo (92)
ykwvu (43)
tfzsp (126) -> zjmhab, kfdtg
mogla (38)
rxfma (8531) -> kxftjs, bpareqw, rlrnjnz
ckdeo (26)
wkurl (41)
huzjje (86)
lcdleci (344)
nqxju (90)
cwxgg (232) -> uylnr, rchhwx, dxgntli
gfcfsf (56)
ysgnao (76)
kvtpc (32)
raaxwq (63)
fewkfib (965) -> pyzwl, fbhnbdw, nqxju
yffxaiy (1014) -> piqgu, zkyiauj, xlufcc
xeefzwg (1740) -> mjgilv, metyk, yonxye
gnitpo (50)
nlewmv (38)
gdhekde (88)
tyxfxu (9)
lmuwznh (7)
chwoyn (18)
mbmaqh (188)
xsulc (19)
hrbsamg (16) -> hhpib, qybyrm, agcpul, azrcn
tpnvx (284) -> qggycuv, ctafsp, ourium
uxgbnjk (90)
nqzvz (13)
kgfldlb (378) -> ajtjym, xnalv, hwevm
rosop (294)
wuogj (411) -> rhnota, gdawyc
hzxiv (176) -> zedqbr, ymfurx, xfapfj
zbiypv (1040) -> tlfbyq, idqaieo, adlvipk
dcytmqp (93)
saykbn (91)
gpikz (41)
rqsjqrg (1937) -> ctidma, kynrf, oxtckvl
bzijz (66)
ozxpbo (137) -> bwqxk, bdgwdt, imzegu, dvjssd, cybhcko
pkowhq (1187) -> zfrsmm, tlskukk, fqkbscn, mlafk
nylhp (171) -> heqvbi, sarwcau
nwejb (313) -> epuiwg, mculw
xfapfj (26)
sntovuq (37)
nehwool (78) -> knwlsi, zmzwttu, dqxwazb, ebxyi
dvjssd (98) -> nbccggn, orydf
oaantta (14)
vzayzaq (17)
zlubkqa (27)
xhkrx (51)
tkqke (1073) -> zjevsui, emsjxz, hgywlzc
nhhyvm (45)
pokjgkl (8)
dpvmn (99)
aokvbbi (40)
bzumy (1522) -> yfkgike, nqata, zheomzg, hooxvp, glrrnl
bpcrtwl (24)
irkhv (92)
tlskukk (1464) -> ixoiuh, jdxth
nbccggn (91)
svzqi (47)
tcmkx (51)
clhyd (220) -> ghmxos, zjxcrv
zdlxv (287) -> eeuohnm, gzocll
eyjdqm (78)
pmlim (78)
nbfbkao (50)
aeiktt (44)
rbgonm (9)
phmmydn (78)
agkgi (156) -> bibly, bttlx, kirowa
zqjqic (1602) -> humqvl, drfeinj, xxbwj
aghou (51)
ysgotr (954) -> xxxxbnh, stfqd, ptamwey
kmvkkze (55)
glhbvi (64)
baimp (158) -> yyyel, mgreg
xcmre (67)
mhhbej (36)
gdawyc (37)
spdsz (68)
pupaehu (99) -> eolilw, czlmv, zlrvs, vrppl
vmuqpj (160) -> aghou, aktswyh
agcpul (98)
hhuhid (71)
pgkeahi (18)
nebwj (94)
glrrnl (52) -> zwake, gtezger
dtqxiic (21)
pkbxhn (62)
qququw (73)
ilxgo (237) -> nwkxd, xsulc, weeriq
ymonx (56)
nqata (122) -> ovgpa, cadrzx
rahsrp (70)
wepvw (69)
kbefh (83)
cpess (98)
pqnkr (186) -> rnoaimc, pkditpt
uqzoo (47) -> kitggwr, kywpr, saykbn, pxifep
pgfomf (12)
vpcfu (54)
srqbs (221)
ixoiuh (14)
eozev (63)
qtqbdp (8)
zsjoz (73)
yflpz (40)
bpgafw (70)
imzegu (190) -> afpxssv, xdwzfh
ygvwdmg (91)
vebli (73)
hshbiaq (91)
humqvl (222)
fozfiqf (52) -> ndqomrl, iwwngux, xaxuyg, cnbvz, wmfro
iukfd (291) -> vcjavrw, mydixq
eduxyw (79)
ygtbl (89)
fjmjc (3192) -> zkhtua, furny, fewkfib
csvxlo (236)
xzdhzoa (45)
avnosfl (87) -> zsraq, lzstd
ptcby (71) -> dapjhx, hyzzwx, kpwfqfc, ivnufgs
hvsywx (170) -> tvhftq, xuokqm
gfsnb (71) -> gtixbu, micfuva
qxkilb (20)
pkesm (92)
ndjjag (59)
vsffvp (5)
ieqgh (247) -> zkdwxd, weejuk, yptrqp
jxevrgz (60)
mnjirpy (24)
hufrsq (3110) -> cmouh, vbdycf, seicwz, pwdoio, bhwnch, ozxpbo
tvhftq (35)";

    timely::execute_from_args(std::env::args(), move |worker| {

        let index = worker.index();
        let peers = worker.peers();

        let worker_input = 
        input
            .split('\n')
            .enumerate()
            .filter(|&(pos,_)| pos % peers == index)
            .map(|(_,line)| {
                let mut words = line.split_whitespace();
                let name = words.next().unwrap().to_string();
                let weight = words.next().unwrap().trim_matches('(').trim_matches(')').parse::<usize>().unwrap();
                words.next();
                let links = words.map(|x| x.trim_matches(',').to_string()).collect::<Vec<_>>();
                (name, weight, links)
            })
            .collect::<Vec<_>>();

        worker.dataflow::<(),_,_>(|scope| {

            let input = scope.new_collection_from(worker_input).1;

            input.flat_map(|(_,_,links)| links)
                 .negate()
                 .concat(&input.map(|(name,_,_)| name))
                 .consolidate()
                 .inspect(|line| println!("part1: {:?}", line.0));

            let weights = input.explode(|(name,weight,_)| Some((name, weight as isize)));
            let parents = input.flat_map(|(name,_,links)| links.into_iter().map(move |link| (link,name.to_string())));

            let total_weights: Collection<_,String> = weights
                .iterate(|inner| {
                    parents.enter(&inner.scope())
                           .semijoin(&inner)
                           .map(|(_, parent)| parent)
                           .concat(&weights.enter(&inner.scope()))
                });

            parents
                .semijoin(&total_weights)
                .map(|(link,name)| (name,link))
                .group(|key, input, output| {
                    if input.len() > 0 {
                        let mut input = input.to_vec();
                        input.sort_by(|x,y| x.1.cmp(&y.1));
                        if input[0].1 != input[input.len()-1].1 {
                            if input[0].1 != input[1].1 {
                                output.push(((), input[0].1));
                            }
                            else {
                                output.push(((),input[input.len()-1].1));
                            }
                        }
                    }
                })
                .inspect(|x| println!("part2: {:?}", x));
        });

    }).unwrap();
}
