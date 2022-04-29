# Sink protocol
This directory contains protocol buffers messages and gRPC service definition that allows you to provide
sink connector of your database that is recognizable by Yandex Data Transfer.

## Why do you need this
For example, you need to put data into *ChadsonDB* from Yandex Cloud-- a new prosperous DB.  
The simple way to do this is to implement this gRPC service, launch it, and give IP + Port to our service!


## Three steps to implement your source endpoint
This is simple: you need to implement three handles:
1. `rpc Spec(SpecReq) returns (SpecRsp) {}`
   First of all, those who would like to use your connector should know, how to tune it. If you are writing MongoDB
   connector, maybe you would like to get some connection string to perform data extraction. So you reply with simple
   [JSON Schema](https://json-schema.org/) specification as configuration:
    ```(json)
   {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "$id": "https://johndoedb.tech/source.connector.schema.json",
      "title": "JohnDoeDbSourceConnectorParameters",
      "description": "A parameters for JohnDoeDB source connector",
      "type": "object"
      "properties": {
        "connection_string": {
          "description": "MongoDB connection string",
          "type": "string",
          "pattern": "mongodb://.+/[a-zA-Z0-9_]+"
        },
      },
      "required": [ "connection_string", "price" ]
   }
    ```
   Thus, you are telling, that the settings is a record with one field: `connection_string` of type string. Note, that
   this is a machine-readable configuration that can be used to create interface, for example.
2. `rpc Check(CheckReq) returns (CheckRsp) {}`
   After getting settings specification in the form of JSON schema, the next step is to configure endpoint with
   valid JSON object. If the JSON settings are valid, this handle should return OK. You should implement
   json schema validation as well as other validation, e.g. check that connection string is a well-formed value.
   Example of configuration:
   ```{ connection_string: "mongodb://user1:certainlyrealpassword@800.555.3.555:27017/junkdb" }```
   Of course this connection string is not valid, simply because IP address is malformed. But you are not limited to
   return error on later stages. Moreover, JSON Schema supports regex validation as shown above.
3. `rpc Write(stream WriteReq) returns (stream WriteRsp) {}`
   This is a streaming method of communication with bidirectional messages. The handle itself is responsible
   for snapshotting data. The  Let's name fetcher as F and your connector code as C. The request that sends F is a
   message consisting of table which we are snapshotting, cursor  which defines range of fetching data and
   a control messages. The sequence of control messages forms the protocol. The flow is the following:
    1. F sends C the `WriteInitConnectionReq` in order to C establish connection, and C returns `WriteInitConnectionRsp`.
       If C established connection and sees requested table, then everything is OK in result, otherwise error is non-empty
    2. Then F sends C the `WriteBeginSnapshotReq` identifying that F begins to upload table. Corresponding `WriteBeginSnapshotRsp`
       is returned. Note, that if you use some kind of transaction taking, or data writing to database associated with
       the snapshot: please put that state into 'snapshot_state' field
    3. Now, the phase has come to meaningful point: F sends series of `WriteDataItemReq` to put table rows as data change items,
       then C sends `DataItemRsp` in response. And that process repeats over and over again.
    4. When snapshotting is over, F sends `WriteDoneShapshotReq` connection, and if any resources were associated as
       identified by non-empty `snapshot_state` -- they should be freed by connector C.
   
   Note, that depending on the type of the transfer, there could be such scenarios:
    1. Snapshot Only: the whole scenario is written correctly, and in point 4, after receiving `WriteDoneShapshotRsp`,
       the connection is closed
    2. Increment Only: only points 1 and 3 are applicable. Connection is not closing until replication is move on.
    3. Snapshot and increment: the whole scenario is written correctly, and after receiving `WriteDoneShapshotRsp` the
       process returns to point 3 again replicating new data events until replication is move on.

Note, that handle 3 is a duplex communication and maintained by gRPC which utilizes HTTP2 communication protocol.
