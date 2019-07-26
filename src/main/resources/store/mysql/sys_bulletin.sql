/*
Navicat MySQL Data Transfer

Source Server         : 10.6.6.106
Source Server Version : 50726
Source Host           : 10.6.6.106:3307
Source Database       : xdata_ingest

Target Server Type    : MYSQL
Target Server Version : 50726
File Encoding         : 65001

Date: 2019-07-23 10:15:13
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for sys_bulletin
-- ----------------------------
DROP TABLE IF EXISTS `sys_bulletin`;
CREATE TABLE `sys_bulletin` (
  `id` bigint(20) NOT NULL COMMENT '公告id',
  `at_time` timestamp NULL DEFAULT NULL COMMENT '公告发生时间',
  `node_address` varchar(255) DEFAULT NULL COMMENT '公告发生的节点地址',
  `level` varchar(8) DEFAULT NULL COMMENT '公告级别(ERROR,INFO,WARN,DEBUG)',
  `category` varchar(64) DEFAULT NULL COMMENT '公告类别',
  `message` text COMMENT '公告详细信息',
  `group_id` varchar(64) DEFAULT NULL COMMENT '公告发生的组件所在组的id',
  `group_name` varchar(128) DEFAULT NULL COMMENT '公告发生的组件所在组的名称',
  `source_id` varchar(64) DEFAULT NULL COMMENT '公告发生所在的组件id',
  `source_name` varchar(128) DEFAULT NULL COMMENT '公告发生所在组件的名称',
  `source_type` varchar(16) DEFAULT NULL COMMENT '公告发生所在组件的类型(PROCESSOR,CONTROLLER_SERVICE,FLOW_CONTROLLER等）',
  KEY `idx_group_id` (`group_id`),
  KEY `idx_at_time` (`at_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='系统中的公告日志';
