name := "Kluster"
 
version := "1.0"
 
scalaVersion := "2.11.6"
 
libraryDependencies ++= {
  Seq(

    "org.apache.spark"              %%  "spark-core"                % "2.2.0"       % "provided",
    "org.apache.spark"              %%  "spark-mllib"               % "2.2.0"       % "provided",
    "org.apache.spark"              %%  "spark-graphx"              % "2.2.0"       % "provided",
    "org.apache.spark"              %%  "spark-sql"                 % "2.2.0"       % "provided",
    "org.apache.spark"              %%  "spark-hive" 		    % "2.2.0"       % "provided",
    "org.apache.spark"              %%  "spark-streaming"           % "2.2.0"       % "provided"
  )
}
