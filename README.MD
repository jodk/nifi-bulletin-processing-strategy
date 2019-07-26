NIFI中日志实时采集到外部系统
----
#功能
 nifi中日志只能在内存中存储5分钟以内的，该工程为了将日志实时对接到
 外部系统，根据不同的存储介质存储到不同的地方，本次只实现存储到mysql中
 如有需要可以添加其他存储介质如elasticsearch

# 安装

## nifi配置

- nifi.properties

在文件中添加一段
参见项目resources目录中的conf/nifi.properties

其中nifi.bulletin.processing.strategy.store为前缀的配置项是动态的，根据实际的
存储介质进行配置
conf/nifi.properties中的配置是mysql的配置项

如果是mysql的外部系统，需要在mysql中创建表,使用：store/mysql/sys_bulletin.sql文件

- nifi-framework-core
在nifi源代码中找到module：nifi-framework-core
路径：nifi-framework-bundle/nifi-framework/nifi-framework-core

1.源码修改：
在其中的package：org.apache.nifi.events中添加三个类:
````
XdataVolatileBulletinRepository
XdataBulletinExtensionDiscoveringManager
XdataBulletinProcessingStrategyBundle
````
三个类的源码在项目的com.sugon.bulletin.nifi.sources包下，直接复制拷贝到
org.apache.nifi.events包中
2.nifi-context.xml文件修改：
在resources目录中找到此文件,将bean：
<bean id="bulletinRepository" class="org.apache.nifi.events.VolatileBulletinRepository" />
修改为：
<bean id="bulletinRepository" class="org.apache.nifi.events.XdataVolatileBulletinRepository">
<constructor-arg ref="nifiProperties" />
</bean>

3.重新打包
将nifi-framework-nar重新编译打包

## 项目打包

在项目根目录下执行

```bash
mvn clean install -DskipTests -f pom.xml
```
> 将打包后`target`中的`nar`文件复制或者软链接
到`nifi`根目录的`./bulletin_processing_strategy`目录（没有则手动创建）下
> 重启nifi即可