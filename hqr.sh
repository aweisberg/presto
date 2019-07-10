export MAVEN_OPTS="-XX:+UnlockDiagnosticVMOptions  -Xmx16G  -XX:+UseG1GC"
export SINGLE_THREAD=n
mkdir -p data
mkdir -p logs
mvn exec:java  -Dexec.mainClass="com.facebook.presto.hive.HiveQueryRunner" -pl presto-hive  -Dexec.classpathScope=test -Duser.timezone=America/Bahia_Banderas  -l ${PWD}/logs -Dexec.args="${PWD}/data" -Dhive.security=legacy
