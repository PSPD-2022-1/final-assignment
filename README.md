# Trabalho Final PSPD 2022.1

## Configuração do ambiente

São necessários:

- OpenJDK 11.0.16
- Maven 3.6.3
- Hadoop 3.3.4
- Spark 3.3.0 com Scala 2.13
- Kafka 3.2.1 com Scala 2.13
- Bash 5.1.4

## Compilação

### Java

Execute o comando `mvn clean package` para que as dependências sejam baixadas,
configuradas no projeto e para que o pacote JAR seja gerado.

## Uso

É necessário configurar as variáveis de ambiente no arquivo
`launch-scripts/env-vars.sh`.

Para executar o consumidor e produtor, usa-se o script
`launch-scripts/run_spark_kafka_consumer_task.sh`. Ele supõe que os serviços
HDFS, Yarn, Zookeeper e Kafka estejam em execução.

A instalação do Hadoop deve estar configurada com a distribuição de tarefas por
meio do Yarn com o distribuidor Spark. Essa configuração pode ser feita
modificando-se a propriedade `mapreduce.framewrok.name` para ter o valor `yarn`
no arquivo `mapred-site.xml`.

```xml
<property>
	<name>mapreduce.framework.name</name>
	<value>yarn</value>
</property>
```

O distribuidor Spark deve ser copiado para o
diretório de classes do Yarn `hadoop-3.3.4/share/hadoop/yarn/lib`. Também é
necessário configurar as seguintes propriedades no `yarn-site.xml`:

```xml
<property>
	<name>yarn.nodemanager.aux-services.spark_shuffle.class</name>
	<value>org.apache.spark.network.yarn.YarnShuffleService</value>
</property>

<property>
	<name>spark.yarn.shuffle.stopOnFailure</name>
	<value>true</value>
</property>

<property>
	<name>spark.shuffle.service.port</name>
	<value>7337</value>
</property>
```
