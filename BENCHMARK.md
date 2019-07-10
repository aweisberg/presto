To run the benchmark.

1. ./mvnw clean install -DskipTests -Dmaven.javadoc.skip=true -Dair.check.skip-all=true -T C1 -pl '!presto-docs,!presto-server-rpm'
2. ./hqr.sh
3. cat benchmark.sql | presto-cli/target/presto-cli-0.221-SNAPSHOT-executable.jar --client-request-timeout 10m --server localhost:8080 --debug > output
4. Repeat benchmark several times to warm up
5. grep -P "CPU|^\d:\d\d$|SELECT" output
