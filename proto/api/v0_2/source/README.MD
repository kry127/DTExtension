# Source protocol
This directory contains protocol buffers messages and gRPC service definition that allows you to provide
source connector of your database that is recognizable by Yandex Data Transfer.

## Why do you need this
For example, you have some data source, e.g. *JohnDoeDB*.  You would like to ingest this data into Yandex Cloud, 
but right now it is not currently supported by Data Transfer.  In order to ingest data to the Yandex Cloud,
you can simply implement this service and specify it as source endpoint in Data Transfer. Then, Data Transfer
will perform gRPC calls in order to extract data, and your code will proxy this request to *JohnDoeDB*,
and after fetching the result from *JohnDoeDB* you are responding to Data Transfer with data that you fetched.


## Five steps to implement your source endpoint
This is simple: you need to implement five handles:
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
3. `rpc Discover(DiscoverReq) returns (DiscoverRsp) {}`
   If configuration is valid, the next step is to use configured endpoint to extract items available for transfer.
   Note, that you can implement stateless service, because all needed control information is travelling through grpc queries 
   After all, you should return a schema for each table in database
4. `rpc Read(stream ReadReq) returns (stream ReadRsp) {}`
   This is a streaming method of communication with bidirectional messages. The handle itself is responsible
   for snapshotting data. The  Let's name fetcher as F and your connector code as C. The request that sends F is a 
   message consisting of table which we are snapshotting, cursor  which defines range of fetching data and 
   a control messages. The sequence of control messages forms the protocol. The flow is the following:
   1. F sends C the `InitConnectionReq` in order to C establish connection, and C returns `InitConnectionRsp`.
      If C established connection and sees requested table, then everything is OK in result, otherwise error is non-empty
   2. Then F sends C the `CursorReq` message in order to clarify the range of snapshotting data. C responds with
      `CursorRsp`, which contains column name and range of values. Most commonly, primary key and min-max values are
      returned in this range.
   3. Then F sends C the `BeginSnapshotReq` identifying that F begins to upload table. Corresponding `BeginSnapshotRsp`
      is returned. Note, that if you use some kind of transaction taking, or data writing to database associated with
      the snapshot: please put that state into 'snapshot_state' field
   4. Now, the phase has come to meaningful point: F sends `DataItemReq` to request table rows as data change items,
      then C sends in response series of `DataItemRsp`. And that process repeats over and over again.
   5. At some point, F will decide to read table snapshot in parallel, so instead of sending `DataItemReq` it will 
      send query `SplitReq` and acquire `SplitRsp` for splitting `Cursor` on parts. Note, that for some cursors F
      can voluntarily split `Cursor` in parts
   6. If F want to parallelize reading after splitting cursor, it opens new streaming connection, makes points 1-2,
      instead of `InitConnectionReq` sends `BeginShardReq` and passes the state of the snapshot (if any). Then 
      second connection works in the same fashion as described in point 4: giving out `DataItemRsp` by `DataItemReq`.
   7. When snapshotting is over, F sends `DoneShardReq` for sharded connections and `DoneShapshotReq` for the
      main connection. If resources are associated as defined by `snapshot_state` variable, they should be freed
      by the main connection. After receiving any of this messages, the stream is closing
5. `rpc Stream(stream StreamReq) returns (stream StreamRsp) {}`
   The same schema as in previous point is applicable here. Message `StreamReq` contains: stream source (one of
   namespace, name, or the whole source), LSN position (from where to read), and control item `StreamControlItemReq` 
   which again defines duplex protocol. Assume the same F -- fetcher, and C -- connector
   1. Fetcher F sends `FixLsnReq` in order to fix position and gets `FixLsnRsp` with fixed position identifier
   2. Then F may send `CheckLsnReq` in order to check, is required message with specified LSN is still alive.
      If it is, C returns `CheckLsnRsp` with OK. But if it is not, `LostRequestedLsnRsp` is returned
   3. Then F usually send `DataItemReq` in order to C send him a series of `DataItemRsp` -- a replication procedure
      which extracts data items. If for some reason during replication th LSN has been lost, the `LostRequestedLsnRsp`
      will be returned instead of ordinary `DataItemRsp`
   4. F may send `RewindLsnReq` to reset cursor and free resources. This can happen when transfer stops, for example.
   
Note, that handles 5 and 6 are duplex communication and maintained by gRPC which utilizes HTTP2 communication protocol.
