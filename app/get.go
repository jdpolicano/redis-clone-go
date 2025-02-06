package main

var GetCommand = Command{"get", get}

var GetArgsParser = NewArgumentsParser().NumPositionals(1)

func get(ctx RequestContext, args []RespValue) {
	parsedArgs, e := GetArgsParser.Parse(args)
	if e != nil {
		ctx.SendError(e.Error())
		return
	}

	key := parsedArgs.GetPos(0)

	data, exists := ctx.DB.Get(key.String())
	if !exists {
		ctx.SendNullBulkString()
		return
	}
	ctx.SendResp(data)
}
