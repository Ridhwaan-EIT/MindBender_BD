#!/bin/bash


cd ~

sudo apt-get update 
sudo apt-get install openssh-server
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
mkdir -p opt
cd opt
sudo wget http://archive.apache.org/dist/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz
tar -xzf hadoop-2.7.3.tar.gz
sudo rm hadoop-2.7.3.tar.gz
cd ~
if [ ! -f ".bash_profile" ]; then
	touch .bash_profile
fi

echo export HADOOP_HOME=~/opt/hadoop-2.7.3 >> .bash_profile
echo export HADOOP_INSTALL=$HADOOP_HOME >> .bash_profile
echo export HADOOP_MAPRED_HOME=$HADOOP_HOME >> .bash_profile
echo export HADOOP_COMMON_HOME=$HADOOP_HOME >> .bash_profile
echo export HADOOP_HDFS_HOME=$HADOOP_HOME >> .bash_profile
echo export YARN_HOME=$HADOOP_HOME >> .bash_profile
echo export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native >> .bash_profile
echo export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin >> .bash_profile

cd ~/opt/hadoop-2.7.3/etc/hadoop

jav_path=$"export JAVA_HOME=~/opt/jdk1.8.0_221"
sed -i "25s@.*@${jav_path}@" hadoop-env.sh

echo '<configuration> 
		<property> 
			<name>fs.default.name</name> 
			<value>hdfs://localhost:9000</value> 
		</property> 
	</configuration>' >> core-site.xml


echo '<configuration>
		<property>
			<name>dfs.replication</name>
			<value>1</value>
		</property>
		<property>
			<name>dfs.name.dir</name>
			<value>file:///~/opt/hadoop-2.7.3/hdfs/namenode</value>
		</property>
		<property>
			<name>dfs.name.dir</name>
			<value>file:///~/opt/hadoop-2.7.3/hdfs/datanode</value>
		</property>
	</configuration> ' >> hdfs-site.xml


echo '<configuration>
	<property>
		<name>yarn.nodemanager.aux-services</name>
		<value>mapreduce_shuffle</value>
	</property>
</configuration>' >> yarn-site.xml


cp mapred-site.xml.template mapred-site.xml

echo '<configuration>
		<property>
			<name>mapreduce.framework.name</name>
			<value>yarn</value>
		</property>
</configuration>' >> mapred-site.xml

cd ~

chmod 777 opt
cd opt
chmod 777 hadoop-2.7.3

cd hadoop-2.7.3
mkdir hdfs
cd hdfs 
mkdir datanode
mkdir namenode
cd ~
source .bash_profile

