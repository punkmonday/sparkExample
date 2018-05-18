INSERT INTO xdwl_bigdatabase.gathertables (TableName,TableNote,TableType,DataBaseType,ExtractLocation,ReceiveTopics,ExtractKeyWords,ExtractionType,ExtractionAmount,SourceIp,SourceDatabase,SourceUser,SourcePassWord,ModifyTime,CreateTime) VALUES 
('t_jc_ryjcxxb','人员基础信息表',2,1,'','topic3','',1,3000,'172.16.20.100','xdwl_bigdatabase','root','root','2018-05-16 09:12:24.000','2018-05-11 17:22:37.000')
,('t_jc_yhjbxx','业户基本信息表',2,1,'','topic5','',1,3000,'172.16.20.100','xdwl_bigdatabase','root','root','2018-05-16 09:10:05.000','2018-05-11 17:26:48.000')
,('t_jc_cljbxx','车辆基本信息表',3,1,'300','topic1','ID',0,500,'172.16.20.100','xdwl_bigdatabase','root','root','2018-05-15 14:25:16.000','2018-05-11 17:35:10.000')
,('bd_his_gps_current','gps实时定位表',2,1,'1000','topic2','ID',0,5000,'172.16.20.100','gpfp','root','root','2018-05-16 09:08:51.000','2018-05-11 17:36:59.000')
,('logininfo','登录信息表',2,1,'500','topic2','ID',0,300,'172.16.20.100','xdwl_bigdatabase','root','root','2018-05-16 09:08:58.000','2018-05-11 10:44:08.000')
,('bd_baseinfo_platform','基本信息表',2,1,'500','topic2','ID',0,300,'172.16.20.100','gpfp','root','root','2018-05-16 14:51:16.000','2018-05-16 14:51:20.000')
;