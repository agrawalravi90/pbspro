#!/bin/bash

_curdir=$(dirname $(readlink -f $0))
_self=${_curdir}/$(basename $0)
cd ${_curdir}

if [ $# -gt 2 ]; then
	echo "syntax: $0 <no. jobs-per-svr>"
	exit 1
fi

if [ "x$1" == "xsubmit" ]; then
	njobs=$2

	. /etc/pbs.conf

	for i in $(seq 1 $njobs)
	do
		/opt/pbs/bin/qsub -koe -- /bin/date > /dev/null
	done
	exit 0
fi

if [ "x$1" == "xsvr-submit" ]; then
	njobs=$2
	users=$(awk -F: '/^(pbsu|tst)/ {print $1}' /etc/passwd)
	_tu=$(awk -F: '/^(pbsu|tst)/ {print $1}' /etc/passwd | wc -l)
	_jpu=$(( njobs / _tu ))

	for usr in ${users}
	do
		( setsid sudo -Hiu ${usr} ${_self} submit ${_jpu} ) &
	done
	wait
	exit 0
fi

njobs=$1
echo "Total jobs: ${njobs}, Total svrs: ${_ts}, Jobs per Svr: ${_jps}"
podman exec ${svr} ${_self} svr-submit ${njobs}
wait
