# BZN bznSparkNeed

项目包含企业标签，个人标签，核赔模型等内容

## 项目编译，发布

bznSparkNeed使用sbt作为项目管理工具。

编译/打包： 
   1. 打开命令行输入窗口，进入项目根目录 <项目所在目录>/sparkMLlib。
   2. 输入 sbt 进入sbt交互模式。
   3. 输入 projects 查看当前项目有哪些模块。 输入 project <模块名> 进入模块操作模式。
   4. 输入 clean 清理缓存， 输入 compile 编译程序， 输入 assembly 将当前模块打包。
   5. 进入 <模块名>/target/scala-2.10/ 即可找到已打好的jar。