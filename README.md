# tikv-coprocessor-client


This client is based on [client-go](https://github.com/tikv/client-go), it's designed to test tikv coprocessor directly.

## Feature

- Insert table record and index record into tikv using tikv grpc interface.
- Send coprocessor request to tikv and parse the response.



## Init the client
To init the coprocessor client we need the pd address.
```go
client, err := coprocessor.NewClient([]string{"127.0.0.1:2379"}, config.Security{})
	if err != nil {
		t.Fatal("create client failed")
	}
```

## Insert data to tikv
Before you insert the data, you need a table id and index id, you can assign it manually or get it by calling [CopClient.GetTableInfo](coprocessor/table.go) method
Examples see: [record_test](./coprocessor/record_test.go) 

## Send Coprocessor Request
Check comment in [cop](coprocessor/cop.go) file, that file defined a lot methods you can call to send coprocessor request.
Example: see [cop_test](coprocessor/cop_test.go)