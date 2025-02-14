package main

var KeysCommand = Command{"keys", keys}

var KeysArgsParser = NewArgumentsParser().NumPositionals(1)

func keys(ctx RequestContext, args []RespValue) {
	parsedArgs, e := GetArgsParser.Parse(args)
	if e != nil {
		ctx.SendError(e.Error())
		return
	}

	pattern := parsedArgs.GetPos(0).String()
	if pattern != "*" {
		ctx.SendError("full pattern matching not yet supported!")
		return
	}

	keys := ctx.KVStore.Keys()
	ctx.SendStringArray(keys)
}
