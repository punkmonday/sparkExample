CREATE TABLE `fieldrangeinfo` (
  `Id` int(11) NOT NULL AUTO_INCREMENT COMMENT '编号',
  `FieldRangeType` varchar(30) DEFAULT NULL COMMENT '字段值分类',
  `FieldRangeValue` varchar(60) DEFAULT NULL COMMENT '分类统一字段值',
  `FieldRange` varchar(300) DEFAULT NULL COMMENT '字段值范围',
  `CreateTime` datetime DEFAULT NULL,
  `ModifyTime` datetime DEFAULT NULL,
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB AUTO_INCREMENT=53 DEFAULT CHARSET=utf8 COMMENT='字段值分类管理';

CREATE TABLE `gatherfieldset` (
  `Id` bigint(19) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `TableId` int(11) DEFAULT NULL COMMENT '表ID',
  `FieldName` varchar(100) NOT NULL COMMENT '字段名',
  `FieldChinaName` varchar(30) NOT NULL COMMENT '字段中文名',
  `DataType` varchar(10) NOT NULL COMMENT '数据类型',
  `MinLength` double(11,2) DEFAULT NULL COMMENT '最小长度',
  `MaxLength` double(11,2) DEFAULT NULL COMMENT '最大长度',
  `IsEmpty` bit(1) DEFAULT NULL COMMENT '是否可空（1.是，0.否）',
  `IsGather` bit(1) DEFAULT NULL COMMENT '是否采集（1.是，0.否）',
  `Regular` varchar(120) DEFAULT NULL COMMENT '字段验证正则表达式',
  `FieldRange` varchar(50) DEFAULT NULL COMMENT '字段归类值分类(如,昆明，昆明市，统一归类为地区编码)',
  `CreateTime` datetime NOT NULL,
  `ModifyTime` datetime NOT NULL,
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB AUTO_INCREMENT=1241 DEFAULT CHARSET=utf8;

CREATE TABLE `gathertables` (
  `Id` int(11) NOT NULL AUTO_INCREMENT COMMENT '编号',
  `TableName` varchar(60) DEFAULT NULL COMMENT '表名称',
  `TableNote` varchar(60) DEFAULT NULL COMMENT '表说明',
  `TableType` int(3) DEFAULT NULL COMMENT '表分类',
  `DataBaseType` int(3) DEFAULT '0' COMMENT '数据库类型(0:oracle;1:mysql;2:sqlsever)默认为0',
  `ExtractLocation` varchar(30) DEFAULT NULL COMMENT '对表抽取数据主键字段的定位，一般为ID或者时间字段',
  `ReceiveTopics` varchar(30) DEFAULT NULL COMMENT 'kafka接收的主题名称',
  `ExtractKeyWords` varchar(30) DEFAULT NULL COMMENT '按表的某个主键进行增量抽取',
  `ExtractionType` bit(1) DEFAULT NULL COMMENT '抽取类型：0-增量，1-全量',
  `ExtractionAmount` int(11) DEFAULT NULL COMMENT '每批次抽取数据的上限',
  `SourceIp` varchar(30) DEFAULT NULL COMMENT '采集数据源IP地址',
  `SourceDatabase` varchar(30) DEFAULT NULL COMMENT '采集数据源数据库',
  `SourceUser` varchar(30) DEFAULT NULL COMMENT '采集数据源用户名',
  `SourcePassWord` varchar(30) DEFAULT NULL COMMENT '采集数据源密码',
  `ModifyTime` datetime DEFAULT NULL COMMENT '修改时间',
  `CreateTime` datetime DEFAULT NULL COMMENT '创建时间',
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB AUTO_INCREMENT=27 DEFAULT CHARSET=utf8 COMMENT='采集数据源管理';

CREATE TABLE `t_jc_cljbxx` (
  `ID` bigint(32) NOT NULL,
  `CLZJ` varchar(36) DEFAULT NULL,
  `ZZJGDM` varchar(10) DEFAULT NULL,
  `CPHM` varchar(21) DEFAULT NULL,
  `CPYS` varchar(5) DEFAULT NULL,
  `CSYS` varchar(20) DEFAULT NULL,
  `CSCD` decimal(10,0) DEFAULT NULL,
  `CSKD` decimal(10,0) DEFAULT NULL,
  `CSGD` decimal(10,0) DEFAULT NULL,
  `RJ` varchar(25) DEFAULT NULL,
  `ZJ` varchar(35) DEFAULT NULL,
  `CZS` decimal(10,0) DEFAULT NULL,
  `CLCP` varchar(50) DEFAULT NULL,
  `CPXH` varchar(50) DEFAULT NULL,
  `FDJH` varchar(50) DEFAULT NULL,
  `FDJGL` decimal(10,0) DEFAULT NULL,
  `CJH` varchar(32) DEFAULT NULL,
  `DPH` varchar(50) DEFAULT NULL,
  `CLSBVINM` varchar(50) DEFAULT NULL,
  `CLLX` varchar(30) DEFAULT NULL,
  `SYLB` varchar(16) DEFAULT NULL,
  `KCLXYDJ` varchar(50) DEFAULT NULL,
  `KCSYLX` decimal(10,0) DEFAULT NULL,
  `PDRQ` date DEFAULT NULL,
  `KCJBYXQ` date DEFAULT NULL,
  `SSPQ` varchar(50) DEFAULT NULL,
  `RLLX` varchar(6) DEFAULT NULL,
  `CLCCRQ` date DEFAULT NULL,
  `XSZDJRQ` date DEFAULT NULL,
  `CLZP` varchar(100) DEFAULT NULL,
  `DLYSZZ` varchar(32) DEFAULT NULL,
  `DLYSZH` varchar(30) DEFAULT NULL,
  `DLYSZFZJG` varchar(100) DEFAULT NULL,
  `DLYSZFZRQ` date DEFAULT NULL,
  `DLYSZYXQQ` varchar(10) DEFAULT NULL,
  `DLYSZYXQZ` varchar(10) DEFAULT NULL,
  `JYFW` varchar(120) DEFAULT NULL,
  `QTJYFW` varchar(120) DEFAULT NULL,
  `CLKW` decimal(10,0) DEFAULT NULL,
  `CLDW` decimal(10,0) DEFAULT NULL,
  `CLXW` decimal(10,0) DEFAULT NULL,
  `HDDW` decimal(10,0) DEFAULT NULL,
  `JYZT` varchar(10) DEFAULT NULL,
  `JJLX` varchar(30) DEFAULT NULL,
  `CYHZ` varchar(50) DEFAULT NULL,
  `SFGKJY` varchar(2) DEFAULT NULL,
  `SFCRJ` varchar(2) DEFAULT NULL,
  `YCZH` varchar(10) DEFAULT NULL,
  `YCJE` decimal(10,0) DEFAULT NULL,
  `QSSJ` date DEFAULT NULL,
  `JZSJ` date DEFAULT NULL,
  `YCNR` varchar(50) DEFAULT NULL,
  `YCFS` varchar(8) DEFAULT NULL,
  `EJWXZT` varchar(4) DEFAULT NULL,
  `NDSHZTDM` varchar(2) DEFAULT NULL,
  `CZXM` varchar(50) DEFAULT NULL,
  `SFZH` varchar(20) DEFAULT NULL,
  `QBSJ` date DEFAULT NULL,
  `CZZZ` varchar(255) DEFAULT NULL,
  `LXDH` varchar(20) DEFAULT NULL,
  `GCZH` varchar(50) DEFAULT NULL,
  `CCRQ` date DEFAULT NULL,
  `GCRQ` varchar(20) DEFAULT NULL,
  `XSLC` decimal(10,0) DEFAULT NULL,
  `ICKH` varchar(30) DEFAULT NULL,
  `JSY1` varchar(30) DEFAULT NULL,
  `JSY2` varchar(30) DEFAULT NULL,
  `JSY3` varchar(30) DEFAULT NULL,
  `FWZH1` varchar(36) DEFAULT NULL,
  `FWZH2` varchar(20) DEFAULT NULL,
  `FWZH3` varchar(20) DEFAULT NULL,
  `DYQX` decimal(10,0) DEFAULT NULL,
  `DYJE` varchar(10) DEFAULT NULL,
  `SFYH` varchar(2) DEFAULT NULL,
  `DYBZ` varchar(200) DEFAULT NULL,
  `JYXKZH` varchar(30) DEFAULT NULL,
  `YHBM` varchar(30) DEFAULT NULL,
  `SSXS` varchar(8) DEFAULT NULL,
  `SSDQ` varchar(30) DEFAULT NULL,
  `NSRQ` date DEFAULT NULL,
  `NSJG` varchar(6) DEFAULT NULL,
  `JSDJ` varchar(6) DEFAULT NULL,
  `JSDJPDRQ` date DEFAULT NULL,
  `JSDJYXQ` date DEFAULT NULL,
  `JDRQ` date DEFAULT NULL,
  `JDQJL` varchar(550) DEFAULT NULL,
  `KHJG` varchar(8) DEFAULT NULL,
  `KHRQ` date DEFAULT NULL,
  `ZFJZRQ` date DEFAULT NULL,
  `BCEWRQ` date DEFAULT NULL,
  `XCEWRQ` date DEFAULT NULL,
  `BCDXRQ` date DEFAULT NULL,
  `DAH` varchar(30) DEFAULT NULL,
  `NFCYYJWH` varchar(2) DEFAULT NULL,
  `CLJYRQZ` date DEFAULT NULL,
  `WXJCRQ` varchar(10) DEFAULT NULL,
  `WXCCRQ` date DEFAULT NULL,
  `WXDW` varchar(100) DEFAULT NULL,
  `QZRQ` date DEFAULT NULL,
  `SFWH` varchar(2) DEFAULT NULL,
  `TXJLSB` varchar(30) DEFAULT NULL,
  `LYCBSBH` varchar(50) DEFAULT NULL,
  `LYCBZ` varchar(500) DEFAULT NULL,
  `JSYXM` varchar(50) DEFAULT NULL,
  `CYZGZH` varchar(100) DEFAULT NULL,
  `XKRQ` date DEFAULT NULL,
  `LYCBSYXQQ` date DEFAULT NULL,
  `LYCBSYXQZ` date DEFAULT NULL,
  `XKJDSBH` varchar(50) DEFAULT NULL,
  `NSYXQ` date DEFAULT NULL,
  `SFNCKY` varchar(2) DEFAULT NULL,
  `NCKYLB` varchar(8) DEFAULT NULL,
  `BZ` varchar(255) DEFAULT NULL,
  `JLLYJGBH` varchar(32) DEFAULT NULL,
  `JLLYJGMC` varchar(32) DEFAULT NULL,
  `JLLYXTBH` varchar(32) DEFAULT NULL,
  `JLLYXTMC` varchar(32) DEFAULT NULL,
  `DXJSSJ` date DEFAULT NULL,
  PRIMARY KEY (`ID`),
  KEY `CPHM_index` (`CPHM`),
  KEY `SSDQ_INDEX` (`SSDQ`),
  KEY `CLLX_INDEX` (`CLLX`),
  KEY `LXDH_INDEX` (`LXDH`),
  KEY `DXJSSJ_INDEX` (`DXJSSJ`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
