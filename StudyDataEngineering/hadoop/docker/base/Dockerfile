FROM centos:7

RUN { \
	echo '#!/bin/sh'; \
	echo 'set -e'; \
	echo; \
	echo 'dirname "$(dirname "$(readlink -f "$(which javac || which java)")")"'; \
	} > /usr/local/bin/docker-java-home \
	&& chmod +x /usr/local/bin/docker-java-home

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.312.b07-1.el7_9.aarch64

ENV JAVA_VERSION=8u3112
ENV JAVA_CENTOS_VERSION=1.8.0.312.b07-1.el7_9.aarch64

RUN set -x \
	&& yum update -y \
	&& yum install which -y \
	&& yum install perl -y \
	&& yum install  -y \
	java-1.8.0-openjdk-$JAVA_CENTOS_VERSION \
	&& JAVA_HOME=$(docker-java-home)\
	&& yum clean all
RUN echo ${JAVA_HOME}

# ENV HADOOP_HOME=/hadoop

#RUN yum install -y \
#	openssh-server openssh-clients openssh-askpass 

# RUN groupadd -g 1000 hadoop \
# 	&& useradd -u 1000 -g hadoop -d /hadoop hadoop\
# 	&& mkdir -p /var/log/hadoop

# ENV PATH=${PATH}:${HADOOP_HOME}/bin
# ENV HADOOP_HOME=${HADOOP_HOME}
# ENV HADOOP_INSTALL=${HADOOP_HOME}
# ENV HADOOP_MAPRED_HOME=${HADOOP_HOME}
# ENV HADOOP_COMMON_HOME=${HADOOP_HOME}
# ENV HADOOP_HDFS_HOME=${HADOOP_HOME}
# ENV HADOOP_LOG_DIR=/var/log/hadoop
# ENV YARN_HOME=${HADOOP_HOME}
# ENV HADOOP_COMMON_LIB_NATIVE_DIR=${HADOOP_HOME}/lib/native
# ENV PATH=$PATH:${HADOOP_HOME}/sbin:${HADOOP_HOME}/bin
# ENV HADOOP_CONF_DIR =${HADOOP_HOME}/etc/hadoop

# USER hadoop
# WORKDIR ${HADOOP_HOME}

# RUN ssh-keygen -q -N "" -t rsa -P '' \
# 	&& cat ${HADOOP_HOME}/.ssh/id_rsa.pub >> ${HADOOP_HOME}/.ssh/authorized_keys

# Set Hadoop version And Download URL
ENV HADOOP_VERSION=3.2.2
ENV HADOOP_URL=http://mirror.apache-kr.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz

RUN curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz \
	# && tar -xvf /tmp/hadoop.tar.gz -C ${HADOOP_HOME} \
	&& tar -xvf /tmp/hadoop.tar.gz -C /opt \
	# && cp -R hadoop-${HADOOP_VERSION} {HADOOP_HOME} \
	# && rm -rf hadoop-${HADOOP_VERSION}} \
	&& rm /tmp/hadoop.tar.gz 


RUN ln -s /opt/hadoop-$HADOOP_VERSION/etc/hadoop /etc/hadoop

RUN mkdir /opt/hadoop-$HADOOP_VERSION/logs

RUN mkdir /hadoop-data

ENV HADOOP_HOME=/opt/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV MULTIHOMED_NETWORK=1
ENV USER=root
ENV PATH $HADOOP_HOME/bin/:$PATH

ADD entrypoint.sh /entrypoint.sh

RUN chmod a+x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]