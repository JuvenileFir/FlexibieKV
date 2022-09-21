#!/bin/bash

pick_exp_data () {
	cat $1 | grep $2 | awk '{gsub(/^'$2'/, "");print}' | awk '{gsub(/^\s+|\s+$/, "");print}'
}

get_exe_time () {
    cat $1 | grep $2 | grep -oP "[\d.]+(?= ms)" | awk '{total += $1} END {print total/NR}'
}

# cat file GET/SET succ/false
get_lantency_ave () {
    cat $1 | grep $2 | grep $3 | grep -oP "[\d.]+(?= ns)" | awk '{total += $1} END {print total/NR}'
}

# cat file HIT_RATIO hit total
calc_hit_ratio () {
	hits=`cat $1 | grep $2 | grep -oP "(?<= $3: )[\d.]+(?= items)" | awk '{total += $1} END {print total}'`
	total=`cat $1 | grep $2 | grep -oP "(?<= $4: )[\d.]+(?= items)" | awk '{total += $1} END {print total}'`
    # awk 'BEGIN{printf "%.2f\n",'$hits'/'$\total'}'
    echo "$hits $total" | awk '{printf ("%0.4f\n", $1/$2)}'
}