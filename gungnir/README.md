Getting started with Gennai
===========================

## Gennai quick start

### Building Gennai

    $ cd gennai/gungnir
    $ mvn clean package -DskipTests=true

### Deploying and starting the standalone server

    $ tar xzvf server/target/dist/gungnir-server-0.0.1-bin.tar.gz
    $ cd gungnir-server-0.0.1/
    $ ./bin/gungnir-server.sh start conf/gungnir-standalone.yaml
    Pidfile: ./bin/../gungnir-server.pid
    Using config file: ./bin/../conf/gungnir-standalone.yaml
    Starting Gungnir server ... STARTED

How to Stop the standalone server

    $ ./bin/gungnir-server.sh stop
    Pidfile: ./bin/../gungnir-server.pid
    Stopping Gungnir server ... STOPPED

### Accessing the server from the console

    $ tar xzvf client/target/dist/gungnir-client-0.0.1-bin.tar.gz
    $ cd gungnir-client-0.0.1/
    $ ./bin/gungnir -l -u root -p gennai
    Gungnir server connecting ...
    Gungnir version 0.0.1 build at 20150116-063247
    Welcome root (Account ID: 3b9665c1aba847c2a332cfd819f76974)
    gungnir> 

Quit the console

    gungnir> quit;

