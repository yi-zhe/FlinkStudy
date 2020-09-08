### 主机规划

|       | node01 | node02 | node03 | Node04 |
| :---: | :----: | :----: | :----: | :----: |
|  NN   |   *    |        |        |        |
|  SNN  |        |   *    |        |        |
|  DN   |        |   *    |   *    |   *    |
|  ZK   |   *    |   *    |   *    |        |
| Kafka |   *    |   *    |   *    |        |
| Flink |   *    |   *    |   *    |   *    |
|       |        |        |        |        |

****

### 基础环境配置

#### 操作系统安装和网络配置参考[这里](https://blog.csdn.net/liuyanggofurther/article/details/51850101)

**几个注意点**

1. 我使用的CentOS6.9版本, 安装方式没有多大区别
2. 虚拟机内存我设置为了1.5G, 因为后面各个软件需要的内存还是比较大, 如果物理机条件允许, 可以设置更大的
3. 安装过程中主机名可以设置, 也可以在下面步骤中设置

#### 关闭防火墙(本步骤及以下操作都是以root用户完成的)

service iptables stop

chkconfig iptables off

#### 关闭selinux

vi /etc/selinux/config

SELINUX=enforcing改为SELINUX=disabled

#### 安装基础软件

##### yum install -y ntp

service ntpd start

chkconfig ntpd on

可能需要重启以保证系统时间的准确性

##### yum install -y nc

##### Jdk1.8

1. 将jdk1.8复制到/opt目录下

2. vi /etc/profile配置环境变量(添加下面两行)

   export JAVA_HOME=/opt/jdk
   export PATH=\$PATH:\$JAVA_HOME/bin

3. :wq保存退出

4. 使环境变量生效 source /etc/profile

5. java -version验证安装是否成功

安装完基础软件后, 我们将虚拟机关机, 通过复制虚拟机的方式, 来创建其他几台机器

### 复制虚拟机

#### 虚拟机复制

1. 在刚刚创建的虚拟机节点上, 右击鼠标, 选中管理, 再选中克隆

2. 选择虚拟机中的当前的状态
3. 选择创建完整克隆
4. 虚拟机名称分别填写Node02/Node03/Node04(我们需要复制三个虚拟机), 路径根据自己想放的位置随意
5. 点击"完成"后等待克隆结束
6. 结束后启动虚拟机

#### 虚拟机复制后需要处理网络的一些配置

复制出来的虚拟机登录后是不能上网的, 需要按照下面步骤处理一下

1. 删除 /etc/udev/rules.d/70-persistent-net.rules 然后重启机器(reboot).文件70-persistent-net.rules确定了MAC地址与网卡的绑定，克隆之后网卡的MAC地址发生了变化，所以导致系统认为网络设备不存在。

2. 重启机器后将/etc/udev/rules.d/70-persistent-net.rules文件最后的’eth1’ 修改为‘eth0’,然后将

   /etc/sysconfig/network-scripts/ifcfg-eth0中的MAC地址修改为与/etc/udev/rules.d/70-persistent-net.rules文件中所写的地址相同，之后再重启机器（reboot）,eth0就可以正常启动了，各种ip和域名也可以ping通了

3. 第二三四台主机的ip改成一个实际可用的ip, 不要和其他机器有冲突
4. 修改主机名

​        vi /etc/sysconfig/network

​        在文件末尾加入HOSTNAME=`你需要的主机名`

​        本次实验需要四台主机, 主机名分别为node01、node02、node03、node04

5. 重启之后就可以联网了

6. 配置host文件, 以便我们可以通过主机名进行互相访问, 对于我们四台虚拟机分别做如下操作

​       vi /etc/hosts

​       根据自己的情况加入

​       ipnode01 node01
​       ipnode02 node02
​       ipnode03 node03
​       ipnode04 node04

#### 配置免密登录

分别在四台主机上执行下面命令

​    ssh-keygen

​    执行后连续三次回车即可

分别在每台机器上对四台机器分别执行

​    ssh-copy-id root@node01 输入yes然后再输入node01上root的密码

​    ssh-copy-id root@node02 输入yes然后再输入node02上root的密码

​    ssh-copy-id root@node03 输入yes然后再输入node03上root的密码

​    ssh-copy-id root@node04 输入yes然后再输入node04上root的密码

执行成功后, 在任意一台机器上以root用户登录其他机器都可以免密的, 就达到了我们的目的

如在任意一台机器上执行(包括node03自己)ssh root@node03都能够无需密码而登录成功

****

### 安装学习Flink需要的软件环境

#### [Hadoop common](https://archive.apache.org/dist/hadoop/common/hadoop-2.9.2/)

我使用的版本是2.6.5.这里只使用完全分布式集群, HA和Federation配置略繁琐, 作为学习环境, 只搭建完全分布式集群, 有兴趣的小伙伴可以自行研究搭建HA和Federation, 各个节点的角色划分, 参考文档开始部分.

**基础环境**

安装文档第一部分基础环境配置后, Hadoop的运行环境就已经足够了

hadoop-2.6.5下载好后, 上传到root@node01:/opt目录下

**部署配置**

1. 配置Hadoop中使用的JAVA_HOME(因为需要远程执行, 所以需要单独配置)

   vi /opt/hadoop-2.6.5/etc/hadoop/hadoop-env.sh

   找到export JAVA_HOME=${JAVA_HOME}替换为

   export JAVA_HOME=/opt/jdk

2. 配置环境变量

   vi /etc/profile

   在export JAVA_HOME=/opt/jdk下面添加

   export HADOOP_HOME=/opt/hadoop-2.6.5

   将export PATH=\$PATH:\$JAVA_HOME/bin后面添加:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin变为

   export PATH=\$PATH:\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin

   source /etc/profile 使配置的环境变量生效

   配置文件分发到其他节点

   cd /etc

   scp profile node02:\`pwd\`

   scp profile node03:\`pwd\`

   scp profile node04:\`pwd\`

   其他软件也会配置环境变量, 配法与此类似, 下面的配置会简略讲解

3. 配置core-site.xml

   配置NameNode节点

   vi $HADOOP_HOME/etc/hadoop/core-site.xml添加

   ```xml
   <property>
     <name>fs.defaultFS</name>
     <value>hdfs://node01:9000</value>
   </property>
   ```

4. 配置hdfs-site.xml

   配置hdfs副本数

   vi $HADOOP_HOME/etc/hadoop/hdfs-site.xml添加

   ```xml
   <property>
     <name>dfs.replication</name>
     <value>2</value>
   </property>
   ```

   配置NameNode数据存储位置, 防止机器重启造成数据丢失(默认在/tmp目录下)

   ```xml
   <property>
     <name>dfs.namenode.name.dir</name>
     <value>/var/bigdata/hadoop/full/dfs/name</value>
   </property>
   ```

   配置DataNode数据存储位置

   ```xml
   <property>
     <name>dfs.datanode.data.dir</name>
     <value>/var/bigdata/hadoop/full/dfs/data</value>
   </property>
   ```

   配置SecondaryNameNode

   ```xml
   <property>
     <name>dfs.namenode.secondary.http-address</name>
     <value>node02:50090</value>
   </property>
   ```

   配置SecondaryNameNode的存储目录

   ```xml
   <property>
     <name>dfs.namenode.checkpoint.dir</name>
     <value>/var/bigdata/hadoop/full/dfs/secondary</value>
   </property>
   ```

5. 配置slaves

   配置DataNode在哪些节点启动(根据文档开始的节点配置)

   vi $HADOOP_HOME/etc/hadoop/slaves添加

   node02

   node03

   node04

6. 将hadoop及配置文件分发到其他节点

   cd /opt

   scp -r hadoop-2.9.2 node02:\`pwd\`

   scp -r hadoop-2.9.2 node03:\`pwd\`

   scp -r hadoop-2.9.2 node04:\`pwd\`

**初始化和启动**

​    格式化(不需要重复操作, 一次即可)

​        hdfs namenode -format

​        出现Storage directory /var/bigdata/hadoop/full/dfs/name has been successfully formatted就成功了

​        格式化生成的fsimage文件和VERSION文件可以自行查看, 这里不做过多讲解

​    启动

​        start-dfs.sh

​    验证

​        当前主机也要配置hosts然后浏览器打开http://node01:50070

​        各个节点上执行jps观察与配置是否一致

### [ZooKeeper](https://downloads.apache.org/zookeeper/zookeeper-3.5.8/)(下载apache-zookeeper-3.5.8-bin.tar.gz)

ZooKeeper使用3.5.8版本

zookeeper-3.5.8下载后上传到root@node01:/opt目录下

1. 配置环境变量

   vi /etc/profile

   export ZOOKEEPER_HOME=/opt/zookeeper-3.5.8

   export PATH=\$PATH:\$ZOOKEEPER_HOME/bin

   保存退出

   source /etc/profile

2. 修改zookeeper配置文件

​        cd /opt/zookeeper-3.5.8/conf

​        mv zoo_sample.cfg zoo.cfg

​        dataDir=/var/bigdata/zk

​        最后一行加上

​        server.1=node01:2888:3888

​        server.2=node02:2888:3888

​        server.3=node03:2888:3888

3. 添加文件

   mkdir -p /var/bigdata/zk

   echo 1 > /var/bigdata/zk/myid

   这个步骤需要在把软件分发到其他节点后, 在node02、node03同样做一次

   在node02执行echo 2 > /var/bigdata/zk/myid

   在node02执行echo 3 > /var/bigdata/zk/myid

4. 分发

   cd /opt

   scp -r zookeeper-3.5.8 node02:\`pwd\`

   scp -r zookeeper-3.5.8 node03:\`pwd\`

   注意需要在node02、node03上执行source /etc/profile

5. 启动验证

   在node01、node02、node03三台机器上分别启动zookeeper.

   zkServer.sh start

   zkServer.sh status 会有两个follower一个leader

   jps会看到有QuorumPeerMain进程

#### [Kafka](https://www.apache.org/dyn/closer.cgi?path=/kafka/2.4.1/kafka_2.11-2.4.1.tgz)

Kafka使用2.11-2.4.1版本

zookeeper-3.5.8下载后上传到root@node01:/opt目录下

1. 配置环境变量

​      export KAFKA_HOME=/opt/zookeeper-3.5.8

​      export PATH=\$PATH:\$KAFKA_HOME/bin

​      保存退出

​      source /etc/profile

2. 修改配置文件

   1. vi /opt/kafka_2.11-2.4.1/config/server.properties

      broker.id=1另外两个节点分别改为2、3

      log.dirs=/var/bigdata/kafka/logs

      zookeeper.connect=node01:2181,node02:2181,node03:2181

3. 分发到node02、node03 注意对应的配置中broker.id需要分别修改为2、3

4. 启动

   三台机器上分别执行

   /opt/kafka_2.11-2.4.1/bin/kafka-server-start.sh /opt/kafka_2.11-2.4.1/config/server.properties &

5. 验证

   jps出现Kafka进程

#### [Flink](https://archive.apache.org/dist/flink/flink-1.9.2/)

下载flink-1.9.2-bin-scala_2.11.tgz

下载后上传到root@node01:/opt目录下

1. 配置环境变量

   vi /etc/profile

   增加export FLINK_HOME=/opt/flink-1.9.2

   export PATH=\$PATH:\$FLINK_HOME/bin

   分发到其他节点

   source /etc/profile

2. 修改配置

   vi /opt/flink-1.9.2/conf/flink-conf.yaml

   jobmanager.rpc.address: node01注意中间有一个空格

   taskmanager.numberOfTaskSlots: 2

   high-availability: zookeeper 打开注释

   high-availability.storageDir: hdfs://node01:9000/flink/ha

   high-availability.zookeeper.quorum: node01:2181,node02:2181,node03:2181

   rest.port: 8081打开注释

3. vi /opt/flink-1.9.2/conf/masters

   node01:8081

   node02:8081

4. vi /opt/flink-1.9.2/conf/slaves

   node02
   node03
   node04

4. 下载支持Hadoop的[插件](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.6.5-10.0/flink-shaded-hadoop-2-uber-2.6.5-10.0.jar[), 下载好后复制到flink-1.9.2/lib下

5. 有个虚拟空间不足的问题

如果yarn-session启动失败了, 可以这样配置 yarn-site.xml

```xml
<property>  
    <name>yarn.nodemanager.vmem-check-enabled</name>  
    <value>false</value>  
</property> 
```

