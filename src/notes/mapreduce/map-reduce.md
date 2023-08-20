## thoughts on the first version
> based on the implementation of 2023-07-19
1. possible to have only 1 task pool that has both map and reduce?
2. change the key-value set to be based on task id instead of filename
3. need to 2 differnt channels?
4. use temp file output

## thoughts on improving the first version
1. only 1 task pool, that assign both Map and Reduce
2. workder request task and finish task individually
3. task to be consumed through channel
3. delete task upon completion
4. use temp file output

## open issues
1. seen "unexpected EOF" when "failed to ask for task", suspect the issue was on the termination of grpc

## reference
1. https://www.cnblogs.com/pxlsdz/p/15408731.html