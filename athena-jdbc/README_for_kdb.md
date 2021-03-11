How to build in offline mode.
```
cd ../athena-federation-sdk
mvn -o -llr clean install -DskipTests -Dmaven.test.skip
cd ../athena-jdbc
mvn -o -llr clean install -DskipTests -Dmaven.test.skip -Dcheckstyle.skip
```