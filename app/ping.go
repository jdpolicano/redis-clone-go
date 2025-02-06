package main

var PingCommand = Command{"ping", ping}

func ping(ctx RequestContext, args []RespValue) {
	ctx.SendSimpleString("PONG")
}
