# binlog2sql_sync_nojson


描述：  
1）通过binlog2sql进行修改，实现实时的binlog增量同步，可以写文件或写库，主要用于数据迁移时的增量同步  
2）通过binlog2sql进行修改，将原先where条件的字段以主键进行替换，避免生成的SQL过长  

安装：  
支持python2.7和python3  
pip install PyMySQL==0.7.11  
pip install mysql-replication==0.13  
pip redis  


限制：  
1）不支持json表的dml的解析，可以支持简单类的json语法，如果超了原生包的解析，则会出错，该错误在binlog2sql也会出现  
2）不支持没有主键的表  

功能：  
1.模拟从节点，用于进行数据迁移时进行增量数据同步  

使用：  
需要redis实时记录position位置，在脚本中修改redis ip:port  

Sample :  
   shell> python binlog2sql_sync_nojson.py -f mysql-bin.000001 --start-position=2585 --outfile=/tmp/1.sql  --host=192.168.1.1 --user=admin --password=123456 --port=3306   
   shell> python binlog2sql_sync_nojson.py -f mysql-bin.000001 --start-position=2585 --outfile=/tmp/1.sql  --host=192.168.1.1 --user=admin --password=123456 --port=3306 --database=test  
   shell> python binlog2sql_sync_nojson.py -f mysql-bin.000001 --start-position=2585 --outfile=/tmp/1.sql  --host=192.168.1.1 --user=admin --password=123456 --port=3306 --database=test --table=t1  
