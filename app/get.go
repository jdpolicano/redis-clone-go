package main

var GetCommand = Command{"get", get}

var GetArgsParser = NewArgumentsParser().NumPositionals(1)

func get(ctx RequestContext, args []RespValue) {
	parsedArgs, e := GetArgsParser.Parse(args)
	if e != nil {
		ctx.SendError(e.Error())
		return
	}

	key := parsedArgs.GetPos(0).String()

	data, dataExists := ctx.KVStore.Get(key)
	expiry, expiryExists := ctx.ExpiryStore.Get(key)

	if !dataExists {
		ctx.SendNullBulkString()
		return
	}

	if expiryExists && expiry.Expired() {
		ctx.SendNullBulkString()
		return
	}

	ctx.SendResp(data)
}
