nohup mysql -u admin -h svc-c8987e07-793c-49da-a15d-13b31b179a6c-ddl.aws-oregon-2.svc.singlestore.com -P 3306 --default-auth=mysql_native_password -pSinglestore01 -Diotdemo -e "call derive_aggs(5)" &
