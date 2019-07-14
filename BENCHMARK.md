To run the benchmark.

1. mvn clean install -DskipTests
2. ./hqr.sh
3. cat benchmark.sql | presto-cli/target/presto-cli-0.221-SNAPSHOT-executable.jar --server localhost:8081 --debug > output
4. Repeat benchmark several times to warm up
5. grep -P "CPU|^\d:\d\d$|SELECT" output

Orri originally ran these queries on his two socket workstation with 4 hyperthreaded cores per socket and these are the numbers in the blog post.

Ariel ran the same queries on a dev server VM with 12 hyperthread cores. With hyperthreading disabled Ariel was able to get consistent results so if you run with hyperthreading you will need to average the result of several measurements.

Orri and Ariel saw differences in the results that persisted even when rechecking on each configuration. The profiles were similar and didn't indicate anything was wrong with what we were measuring and the results were as expected given the query and the advantages Aria had given how it would execute the query.

Here is the ratio of baseline CPU time to Aria CPU time as measure by Ariel and Orri. This is all queries in order from benchmark.sql except for the ones run with filter reordering enabled.

| Ariel | Orri | 
| ----- | ---- | 
| 1.51  | 2.04 |
| 1.5   | 1.73 |
| 1.45  | 1.26 |
| 1.58  | 1.45 |
| 1.84  | 1.84 |
| 3.4   | 3.65 |
| 6.74  | 6.13 |
| 1.29  | 1.37 |

Orri ascribed the differences to the substantially different hardware in terms of memory bandwidth and CPU clock. Orri's workstation was clocked at 3.5GHz while Ariel's configuration was clocked at 2.4GHz. Orri's two socket configuration also had several additional memory channels.

If you are taking the time to test this it would be great to know what results you saw on your particular hardware.
