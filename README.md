## Prototype

This is a key-value store prototype that implements several popular Erasure Codes via Jerasure library, including operations of encoding, decoding, partial-encoding and partial-decoding  (`encoding-and-transferring` with two implementation methods).

The architecture follows master-worker style, like many state-of-art distributed file storage such as CubeFS, HDFS and Ceph. Four major components are client, coordinator, proxy and datanode. 

- `Erasure Codes` include

  - Reed-Solomon Codes
    - basic RS Code

  - Locally Repairable Codes (LRCs)
    - Azure-LRC (Microsoft's Azure LRC)
    - Azure-LRC+1
    - Optimal-LRC
    - Optimal-Cauchy-LRC (Google's LRC)
    - Uniform-Cauchy-LRC (Google's LRC)

  - Product Codes
    - basic Product Code
    - HVPC, Product Code without global parity blocks

- Operations include

  - set/write
  - get/read, including solutions for degraded read
  - delete
  - block repair, node repair
  - stripe merging
    - now support `RS Codes`, `Azure-LRC`, `PCs` with `optimal` placement rule

- Implementations for storing data

  - in disk (for large-size objects)
  - in memory (for small-size objects)
    - a simple kv-map (default)
    - via `redis`
    - via `memcached`

  > for different implementation, you should assign the correct value of `IN_MEMORY`, `IF_REDIS`, `IF_MEMCACHED` in the following files: `project/include/metadata.h`, `project/CmakeLists.txt`, `install_third_party.sh`, `tools/generator_sh.py`.

## Quick Start

### Environment Configuration

- Required `gcc` version

  - At least `gcc 11.1.0`

- Required packages

  * yalantinglibs

  * gf-complete & jerasure

  * If using redis for storage
    * hredis & redis-plus-plus & redis

  * If using memcached for storage
    * libmemcached & memcached

- we call them third_party libraries, run the following command to install these packages

  ```shell
  sh install_third_party.sh
  ```

### Compile and Run

- Complie

```
cd project/
sh compile.sh
```

- Generate `xml` file for cluster information and shell files for run

```shell
cd tools/
# edit
python generator_sh.py
```

- Edit `project/config.ini`

```ini
[ECSCHEMA]
# if apply encoding-and-transferring
partial_decoding = true
# partial decoding scheme
partial_scheme = false
# repair priority for LRCs. If true, first choose global parity for repair; If not, local first
repair_priority = false
# methods for repairing multi-blocks in LRCs
# If false, data + parity; If true, first data, then recalculate parity
repair_method = false

# erasure-coded type, one of the following value
# for RS Code: RS
# for LRCs: AZURE_LRC, AZURE_LRC_1, OPTIMAL_LRC, OPTIMAL_CAUCHY_LRC, UNIFORM_CAUCHY_LRC
# for Product Codes: PC, HV_PC
ec_type = RS

# placement rule of a single stripe: FLAT, RANDOM, OPTIMAL
placement_rule = OPTIMAL

# placement rule of every x stripes
# for RS Codes and LRCs: RAND, DISPERSED, AGGREGATED
# for Product Codes: VERTICAL, HORIZONTAL
multistripe_placement_rule = RAND

# KB, block_size
block_size = 1

# number of stripes to be merged into a larged-size stripe
x = 2

# number of data blocks in a stripe
k = 6

# for RS Codes
# number of parity blocks in a stripe
m = 4

# for LRCs
# number of local parity blocks in a stripe
l = 2
# number of global parity blocks in a stripe
g = 2

# for Product Codes
# number of data blocks in a row
k1 = 3
# number of parity blocks in a row
m1 = 1
# number of data blocks in a column
k2 = 2
# number of parity blocks in a column
m2 = 1
```

- Run

  - run in a local node for test

    ```
    sh run_proxy_datanode.sh
    ./project/build/run_coordinator
    ./project/build/run_client [config_file_name] [tracefile_name] [number_of_stripes] [number_of_failures]
    ```

  - run in multiple nodes for testbed experiments

    - first edit and run `exp.sh` with argurement `2` to move necessary files to other nodes

    - then

      ```
      sh run_server.sh
      sh run_client.sh
      ```

### Some Helpful Tips

#### Install `gcc-11`&`g++-11` in global environment on `Ubuntu 20.04`

1. add the install source

   ```shell
   sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
   ```

2. install `gcc-11`&`g++-11`

   ```shell
   sudo apt-get install gcc-11 g++-11
   ```

3. verify

   ```shell
   gcc-11 -v
   g++-11 -v
   ```

4. set the priority of different `gcc` versions, the one with the highest score is regarded as the default one, for example

   ```
   sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 100 --slave /usr/bin/g++ g++ /usr/bin/g++-11 --slave /usr/bin/gcov gcov /usr/bin/gcov-11
   sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 95 --slave /usr/bin/g++ g++ /usr/bin/g++-10 --slave /usr/bin/gcov gcov /usr/bin/gcov-10
   sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 90 --slave /usr/bin/g++ g++ /usr/bin/g++-9 --slave /usr/bin/gcov gcov /usr/bin/gcov-9
   ```

5. modify the default version, run the command and choose the version

   ```
   sudo update-alternatives --config gcc
   ```

#### Install `gcc-11`&`g++-11` in user's local environment

- a shell script

```shell
PACKAGE_DIR=/path/to/where/tar_package/in
INSTALL_DIR=/path/to/install
GCC_DIR=$PACKAGE_DIR/gcc-$GCC_VERSION
GCC_INSTALL_DIR=$INSTALL_DIR/gcc-$GCC_VERSION

mkdir -p $GCC_INSTALL_DIR
cd $GCC_INSTALL_DIR
rm -rf *
cd $PACKAGE_DIR
rm -rf gcc-$GCC_VERSION
if [ ! -f "gcc-${GCC_VERSION}.tar.gz" ]; then
	wget --no-check-certificate https://gcc.gnu.org/pub/gcc/releases/gcc-${GCC_VERSION}/gcc-${GCC_VERSION}.tar.gz
fi
tar -xvzf gcc-${GCC_VERSION}.tar.gz
cd $GCC_DIR
./contrib/download_prerequisites
./configure --prefix=$GCC_INSTALL_DIR --disable-multilib --enable-checking=release 
make -j6
make install
```

- set up the environment

```shell
TEMPORARY_SETTING=0

if [ ${TEMPORARY_SETTING} -eq 1 ]; then
	export PATH=${GCC_INSTALL_DIR}/bin:\$PATH
	export CC=${GCC_INSTALL_DIR}/bin/gcc
	export CXX=${GCC_INSTALL_DIR}/bin/g++
	export LIBRARY_PATH=${GCC_INSTALL_DIR}/lib:$LIBRARY_PATH
	export LD_LIBRARY_PATH=${GCC_INSTALL_DIR}/lib64:$LD_LIBRARY_PATH
else
	sudo echo "" >> ~/.bashrc
	sudo echo "export PATH=${GCC_INSTALL_DIR}/bin:\$PATH" >> ~/.bashrc
	sudo echo "export CC=${GCC_INSTALL_DIR}/bin/gcc" >> ~/.bashrc
	sudo echo "export CXX=${GCC_INSTALL_DIR}/bin/g++" >> ~/.bashrc
	sudo echo "export LIBRARY_PATH=${GCC_INSTALL_DIR}/lib:$LIBRARY_PATH" >> ~/.bashrc
	sudo echo "export LD_LIBRARY_PATH=${GCC_INSTALL_DIR}/lib:${GCC_INSTALL_DIR}/lib64:\$LD_LIBRARY_PATH" >> ~/.bashrc
	source ~/.bashrc
fi
```

#### About installing `GLIBC_2.18` safely

Running project complied by`gcc-11` may need `GLIBC_2.18`, if the system hasn't installed the updated version of `GLIBC`, install it to your local environment using the script.

```shell
PACKAGE_DIR=/path/to/where/tar_package/in
INSTALL_DIR=/path/to/install
GLIBC_DIR=$PACKAGE_DIR/glibc-2.18
GLIBC_INSTALL_DIR=$INSTALL_DIR/glibc-2.18

mkdir -p $GLIBC_INSTALL_DIR
cd $GLIBC_INSTALL_DIR
rm -rf *
cd $PACKAGE_DIR
rm -rf glibc-2.18
if [ ! -f "glibc-2.18.tar.gz" ]; then
	wget --no-check-certificate https://mirrors.tuna.tsinghua.edu.cn/gnu/glibc/glibc-2.18.tar.gz
fi
tar -xvzf glibc-2.18.tar.gz
cd $GLIBC_DIR
./glibc-2.18/configure --prefix=$GLIBC_INSTALL_DIR
make -j6
make install
```

then compile the project with flag `-Wl,--rpath=` and `-Wl,--dynamic-linker=`, when using `CmakeLists.txt`, add the following sentences

```cmake
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--rpath=/path/to/install/glibc-2.18/lib -Wl,--dynamic-linker=/path/to/install/glibc-2.18/lib/ld-2.18.so")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--rpath=/path/to/install/glibc-2.18/lib -Wl,--dynamic-linker=/path/to/install/glibc-2.18/lib/ld-2.18.so")
```

