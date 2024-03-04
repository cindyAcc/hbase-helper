# hbase-custom-filter
1.本Demo是针对hbase每row中的各column做的filter
2.注意protobuf版本
3.`.proto` 文件的字段与相应Filter文件中字段要一致
4.生成`DemoFilterProtoV1` 的命令(windows 在protoc.exe目录下代开powershell 将DemoFilter.proto 文件也放在当前目录下)  `./protoc.exe ./DemoFilter.proto --java_outer=./`
    如忘记命令 则输入 `./protoc.exe -h` 查看命令参数
5.自定义Filter写完后需将代码用maven打包成jar包（jar包文件放至hbase /bin 目录下  业务系统如需引用则也需将jar上传至业务系统的maven仓库中）