# Multicharts绩效上传器

## 安装方式：
1.下载项目代码：
```
git clone https://github.com/xingetouzi/McPerfomeranceUploader.git
```

2. 切换或新建python虚拟环境

3. 安装依赖模块
```
pip install -r requirements.txt
```

## 使用方式：
1. 在connection_local.json文件中填入所要导入的mongodb地址
```
{
    "host": "127.0.0.1", # 服务器地址 
    "port": 27017, # 端口
    "user": "root", # 账户
    "password": "123456" #　密码
}
```

2. 切换到安装好python依赖的虚拟环境

3. 运行window.py
