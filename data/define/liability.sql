/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50617
Source Host           : localhost:3306
Source Database       : analyst

Target Server Type    : MYSQL
Target Server Version : 50617
File Encoding         : 65001

Date: 2020-11-12 22:41:36
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for liability
-- ----------------------------
DROP TABLE IF EXISTS `liability`;
CREATE TABLE `liability` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `y` int(11) DEFAULT NULL,
  `ratio` decimal(8,6) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_liability_y` (`y`)
) ENGINE=InnoDB AUTO_INCREMENT=32 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of liability
-- ----------------------------
INSERT INTO `liability` VALUES ('1', '1990', '5.000000');
INSERT INTO `liability` VALUES ('2', '1991', '14.000000');
INSERT INTO `liability` VALUES ('3', '1992', '14.000000');
INSERT INTO `liability` VALUES ('4', '1993', '14.000000');
INSERT INTO `liability` VALUES ('5', '1994', '15.000000');
INSERT INTO `liability` VALUES ('6', '1995', '15.000000');
INSERT INTO `liability` VALUES ('7', '1996', '13.060000');
INSERT INTO `liability` VALUES ('8', '1997', '10.170000');
INSERT INTO `liability` VALUES ('9', '1998', '7.860000');
INSERT INTO `liability` VALUES ('10', '1999', '3.783000');
INSERT INTO `liability` VALUES ('11', '2000', '3.140000');
INSERT INTO `liability` VALUES ('12', '2001', '3.140000');
INSERT INTO `liability` VALUES ('13', '2002', '2.467500');
INSERT INTO `liability` VALUES ('14', '2003', '2.630000');
INSERT INTO `liability` VALUES ('15', '2004', '3.094000');
INSERT INTO `liability` VALUES ('16', '2005', '3.726000');
INSERT INTO `liability` VALUES ('17', '2006', '3.500000');
INSERT INTO `liability` VALUES ('18', '2007', '4.300000');
INSERT INTO `liability` VALUES ('19', '2008', '6.000000');
INSERT INTO `liability` VALUES ('20', '2009', '5.000000');
INSERT INTO `liability` VALUES ('21', '2010', '4.600000');
INSERT INTO `liability` VALUES ('22', '2011', '6.060000');
INSERT INTO `liability` VALUES ('23', '2012', '6.000000');
INSERT INTO `liability` VALUES ('24', '2013', '5.410000');
INSERT INTO `liability` VALUES ('25', '2014', '5.410000');
INSERT INTO `liability` VALUES ('26', '2015', '5.000000');
INSERT INTO `liability` VALUES ('27', '2016', '4.300000');
INSERT INTO `liability` VALUES ('28', '2017', '4.200000');
INSERT INTO `liability` VALUES ('29', '2018', '4.270000');
INSERT INTO `liability` VALUES ('30', '2019', '4.270000');
INSERT INTO `liability` VALUES ('31', '2020', '3.970000');



## index weigth data
INSERT INTO `index_weight`
(`index_code`, `con_code`, `y`, `m`, `trade_date`, `weight`) VALUES
('tangchao', '600519.SH', '2019', '12', '2019-12-31', '2.425000'),
('tangchao', '002304.SZ', '2019', '12', '2019-12-31', '2.425000'),
('tangchao', '002415.SZ', '2019', '12', '2019-12-31', '2.425000'),
('tangchao', '002027.SZ', '2019', '12', '2019-12-31', '2.425000'),
('tangchao', '000596.SZ', '2019', '12', '2019-12-31', '2.425000');


# calc_val table
CREATE TABLE `calc_val` (
                            `ts_code` varchar(10) NOT NULL,
                            `aaa` float DEFAULT NULL,
                            `inflation` float DEFAULT NULL,
                            `y` int NOT NULL,
                            `m` int NOT NULL,
                            `y_p_1` float DEFAULT NULL,
                            `y_p_2` float DEFAULT NULL,
                            `y_p_3` float DEFAULT NULL,
                            `y_p_sustain` float DEFAULT NULL,
                            `calc_val` float DEFAULT NULL,
                            PRIMARY KEY (`ts_code`,`y`,`m`),
                            KEY `idx_calc_val_m` (`m`),
                            KEY `idx_calc_val_y` (`y`),
                            KEY `idx_calc_val_ts_code` (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
