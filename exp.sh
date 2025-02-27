ARRAY=('node2' 'node3' 'node5' 'node6' 'node7' 'node8' 'node9' 'node10' 'node11' 'node12' 'node18')
NUM=${#ARRAY[@]}
echo "cluster_number:"$NUM
NUM=`expr $NUM - 1`
SRC_PATH1=/home/GuanTian/ec_prototype/run_cluster_sh/
SRC_PATH2=/home/GuanTian/ec_prototype/project
SRC_PATH3=/home/GuanTian/wondershaper

DIS_DIR1=/home/GuanTian/ec_prototype
DIS_DIR2=/home/GuanTian/wondershaper

IF_SERVER=0
IF_REDIS=0
SERVER='redis-server'
if [ $IF_REDIS -eq 0 ]; then
    SERVER='memcached'
fi

# if simulate cross-cluster transfer
if [ $1 == 1 ]; then
    echo "cluster_number:"${#ARRAY[@]}
    for i in $(seq 0 $NUM)
    do
        temp=${ARRAY[$i]}
        echo $temp
        ssh GuanTian@$temp 'cd /home/GuanTian/ec_prototype;bash cluster_run_datanode.sh;'
        echo 'server&datanode process number:'
        ssh GuanTian@$temp 'ps -aux | grep' ${SERVER} '| wc -l;ps -aux | grep run_datanode | wc -l;'
    done
    for i in $(seq 0 $NUM)
    do
        temp=${ARRAY[$i]}
        echo $temp
        if [ $temp != 'node18' ]; then
          ssh GuanTian@$temp 'cd /home/GuanTian/ec_prototype;bash cluster_run_proxy.sh;'
          echo 'proxy process number:'
          ssh GuanTian@$temp 'ps -aux | grep run_proxy | wc -l'
        fi
    done
elif [ $1 == 5 ]; then
    ssh GuanTian@node18 'sudo ./wondershaper/wondershaper/wondershaper -c -a ib0;sudo ./wondershaper/wondershaper/wondershaper -a ib0 -d 1000000 -u 1000000'
elif [ $1 == 6 ]; then
    ssh GuanTian@node18 'sudo ./wondershaper/wondershaper/wondershaper -c -a ib0;echo done'
else
    echo "cluster_number:"${#ARRAY[@]}
    for i in $(seq 0 $NUM)
    do
    temp=${ARRAY[$i]}
        echo $temp
        if [ $1 == 0 ]; then
            if [ $IF_SERVER == 1 ]; then
              if [ $temp == 'node18' ]; then
                  ssh GuanTian@$temp 'pkill -9 run_datanode;pkill -9' ${SERVER}
              else
                  ssh GuanTian@$temp 'pkill -9 run_datanode;pkill -9 run_proxy;pkill -9' ${SERVER}
              fi
            else
              if [ $temp == 'node18' ]; then
                  ssh GuanTian@$temp 'pkill -9 run_datanode;'
              else
                  ssh GuanTian@$temp 'pkill -9 run_datanode;pkill -9 run_proxy'
              fi
            fi
            echo 'pkill  all'
            ssh GuanTian@$temp 'ps -aux | grep' ${SERVER} '| wc -l'
            ssh GuanTian@$temp 'ps -aux | grep run_datanode | wc -l'
            ssh GuanTian@$temp 'ps -aux | grep run_proxy | wc -l'
        elif [ $1 == 2 ]; then
            ssh GuanTian@$temp 'mkdir -p' ${DIS_DIR1}
            ssh GuanTian@$temp 'mkdir -p' ${DIS_DIR2}
            rsync -rtvpl ${SRC_PATH1}${i}/cluster_run_datanode.sh GuanTian@$temp:${DIS_DIR1}
            rsync -rtvpl ${SRC_PATH1}${i}/cluster_run_proxy.sh GuanTian@$temp:${DIS_DIR1}
            rsync -rtvpl ${SRC_PATH2} GuanTian@$temp:${DIS_DIR1}
            rsync -rtvpl ${SRC_PATH3} GuanTian@$temp:${DIS_DIR2}
        elif [ $1 == 3 ]; then   # if not simulate cross-cluster transfer
            ssh GuanTian@$temp 'sudo ./wondershaper/wondershaper/wondershaper -c -a ib0;sudo ./wondershaper/wondershaper/wondershaper -a ib0 -d 1000000 -u 1000000'
        elif [ $1 == 4 ]; then
            ssh GuanTian@$temp 'sudo ./wondershaper/wondershaper/wondershaper -c -a ib0;echo done'
        elif [ $1 == 7 ]; then
            ssh GuanTian@$temp 'cd /home/GuanTian/ec_prototype/storage/;rm -r *'
        fi
    done
fi