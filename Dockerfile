FROM centos:centos6

RUN yum install -y java-1.7.0-openjdk-devel wget tar
RUN \
  mkdir -p /opt/maven && \
  cd /opt/maven && \
  wget 'http://mirrors.ibiblio.org/apache/maven/maven-3/3.2.1/binaries/apache-maven-3.2.1-bin.tar.gz' && \
  tar zxvf apache-maven-3.2.1-bin.tar.gz

ENV JAVA_HOME /usr/lib/jvm/jre-1.7.0-openjdk.x86_64
ENV M2_HOME /opt/maven/apache-maven-3.2.1
ENV PATH /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/rvm/rubies/ruby-1.9.3-p547/bin:/opt/maven/apache-maven-3.2.1/bin
