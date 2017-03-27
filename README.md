## 基于Hadoop和HBase的大规模海量数据去重

## 目录

data - 数据集

docs - 文档

src - MapReduce

## 环境

Hadoop版本1.1.2

HBase 0.94.8

## Usage

搭建好HBase集群和Hadoop集群后，准备输入文件

1、直接运行FSPChunkLevelDedup去重程序

2、运行ToHAFile将输入文件转化一个单独大文件(运行前要拿到批量小文件的信息)

3、运行ReduceAnalyzer对reduce文件统计数据块数目与你的输入文件对比，以计算去重率（本程序硬编码，请自行设置其他参数）