本demo用于演示如何从AWS建立数据仓库。

### 环境配置
1. 创建AWS 账号与IAM，记录KEY与SECRET
2. 修改aws-dwh.cfg,设置KEY与SECRET的值
3. python3.5+ 安装相关包

```shell
pip install -r requirements.txt
```

### 创建Redshift集群

```shell
python create_redshift.py -create
```
执行完上述命令后，可能需要5-10min钟的时间以等待AWS创建集群。注意，为了演示方便，该集群被设定为任意IP都可以访问，这在生产环境中是绝对是严格禁止的。

```shell
python create_redshift.py -state
```
用该命令检查Redshift 集群状态。必须等待集群状态为 available 才可正常进行接下来的操作。

### 从S3加载数据到Redshift作为staging表

```shell
python etl.py -copy
```

### 对staging表进行清洗，得到事实表与维度表 fact & dimension table
```shell
python etl.py -insert
```


### 取样，检查数据是否正常
```shell
python etl.py -sample
```

### 删除数据表并停止集群
重要：不停止集群会一直扣费！

```shell 
python etl.py -drop
python create_redshift.py -delete
```

```shell
python create_redshift.py -state
```
确定集群进入删除状态。