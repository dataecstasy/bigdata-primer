# Hive Interview Questions
### 1. How to improve hive performance with Hadoop.
**Use Tez Engine**

```set hive.execution.engine=tez;```

**Use Vectorization**

```set hive.vectorization.execution.enabled=true;

 set hive.vectorization.execution.reduced.enabled=true;```

### 2. How to list the partitions in a hive table.
```hive> show partitions table_name;```
