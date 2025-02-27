# run client
./project/build/run_client 5 config.ini
# kill coordinator
pkill -9 run_coordinator
# unlimit bandwidth
sh exp.sh 4
# kill datanodes and proxies
sh exp.sh 0

