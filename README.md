# Data Transfer Buffer - Producer-Consumer using Semaphores (Java)

Producer-Consumer problem solved with `java.util.concurrent.Semaphore`.

- `empty` semaphore - producer blocks when buffer is full
- `full` semaphore  - consumer blocks when buffer is empty
- `mutex` semaphore - only one thread touches the buffer at a time
- Buffer contents are displayed after every produce/consume operation.

## Run
```
cd src
javac ProducerConsumer.java
java ProducerConsumer
```
