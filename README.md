# spark-repo
To submit file on your local machine docker run -dit -p 3000:8080 -v /home/eguo/work/de_programming/projectfinal/wcd-engine:/wcd-engine -v /home/eguo/data/:/data/ hseeberger/scala-sbt:8u222_1.3.5_2.13.1

bin/spark-submit --class Driver.MainApp --conf spark.ui.port=5000 --master local /wcd-engine/target/scala-2.12/spark-engine_2.12-0.0.1.jar -p wcd-demo -i Csv -o parquet -s file:/data/banking.csv -d file:/data/wcd_final_folder -m append -c job --input-options header=true
