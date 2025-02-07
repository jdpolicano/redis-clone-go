package main

var ConfigCommand = Command{"config", config}

var ConfigArgsParser = NewArgumentsParser().NumPositionals(2) // eventually this would need to be arbitraility deep

func config(ctx RequestContext, args []RespValue) {
	parsedArgs, e := ConfigArgsParser.Parse(args)
	if e != nil {
		ctx.SendError(e.Error())
		return
	}

	req, key := parsedArgs.GetPos(0), parsedArgs.GetPos(1)
	if !req.EqualAsciiInsensitive("get") {
		ctx.SendError("config command currently only supports get operations...")
		return
	}

	v, exists := ctx.Config.Get(key.String())
	if !exists {
		ctx.SendNullBulkString()
		return
	}
	respArr := getKeyValueArray(key.String(), v)
	ctx.SendResp(respArr)
}

func getKeyValueArray(k string, v string) RespValue {
	kr := RespValue{BulkString, []byte(k)}
	vr := RespValue{BulkString, []byte(v)}
	return RespValue{Array, []RespValue{kr, vr}}
}
