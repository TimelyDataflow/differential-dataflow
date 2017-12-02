extern crate timely;
extern crate differential_dataflow;

// taken from: https://adventofcode.com/2017/day/2

use differential_dataflow::input::Input;
use differential_dataflow::operators::*;

fn main() {

    let input = 
"86  440 233 83  393 420 228 491 159 13  110 135 97  238 92  396
3646    3952    3430    145 1574    2722    3565    125 3303    843 152 1095    3805    134 3873    3024
2150    257 237 2155    1115    150 502 255 1531    894 2309    1982    2418    206 307 2370
1224    343 1039    126 1221    937 136 1185    1194    1312    1217    929 124 1394    1337    168
1695    2288    224 2667    2483    3528    809 263 2364    514 3457    3180    2916    239 212 3017
827 3521    127 92  2328    3315    1179    3240    695 3144    3139    533 132 82  108 854
1522    2136    1252    1049    207 2821    2484    413 2166    1779    162 2154    158 2811    164 2632
95  579 1586    1700    79  1745    1105    89  1896    798 1511    1308    1674    701 60  2066
1210    325 98  56  1486    1668    64  1601    1934    1384    69  1725    992 619 84  167
4620    2358    2195    4312    168 1606    4050    102 2502    138 135 4175    1477    2277    2226    1286
5912    6261    3393    431 6285    3636    4836    180 6158    6270    209 3662    5545    204 6131    230
170 2056    2123    2220    2275    139 461 810 1429    124 1470    2085    141 1533    1831    518
193 281 2976    3009    626 152 1750    1185    3332    715 1861    186 1768    3396    201 3225
492 1179    154 1497    819 2809    2200    2324    157 2688    1518    168 2767    2369    2583    173
286 2076    243 939 399 451 231 2187    2295    453 1206    2468    2183    230 714 681
3111    2857    2312    3230    149 3082    408 1148    2428    134 147 620 128 157 492 2879";

    timely::execute_from_args(std::env::args(), move |worker| {

        let index = worker.index();
        let peers = worker.peers();

        let worker_input = 
        input
            .split('\n')
            .enumerate()
            .filter(move |&(row, _line)| row % peers == index)
            .flat_map(|(row, line)| 
                line.split_whitespace()
                    .enumerate()
                    .map(move |(col, int)| (int.parse::<isize>().unwrap(), row, col))
            );

        worker.dataflow::<(),_,_>(|scope| {

            let entries = scope.new_collection_from(worker_input).1;

            entries
                .map(|(val, row, _col)| (row, val))
                .group(|_row, vals, output| {

                    // Part one solution (difference of max and min within rows)
                    let mut min = vals[0].0;
                    let mut max = vals[0].0;
                    for &(val, _) in vals.iter() {
                        if min > val { min = val; }
                        if max < val { max = val; }
                    }
                    output.push((1, (max - min)));

                    // Part two solution (ratio of two even divisors in the row)
                    if vals.iter().filter(|&&(_,count)| count > 1).next().is_some() {
                        output.push((2, 1));
                    }
                    else {
                        for &(val1, _) in vals.iter() {
                            for &(val2, _) in vals.iter() {
                                if (val1 % val2) == 0 && val1 != val2 {
                                    output.push((2, val1 / val2));
                                }
                            }
                        }
                    }
                })
                .map(|(_, part)| part)
                .consolidate()
                .inspect(|elt| println!("part {} checksum: {:?}", elt.0, elt.2));
        });

    }).unwrap();
}