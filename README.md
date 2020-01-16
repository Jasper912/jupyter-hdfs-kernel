## Jupyter Hdfs Kernel

### Install

```
pip install jupyter_hdfs_kernel
jupyter-kernelspec install hdfs_kernel/kernels/hdfs
```

### User config

* webhdfs name service config to every user

```
cp hdfs_kernel/config.json.template /home/{user}/.hdfs/config.json
```
