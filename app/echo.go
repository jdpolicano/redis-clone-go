package main

var EchoCommand = Command{"echo", echo}

var EchoArgsParser = NewArgumentsParser().NumPositionals(1)

func echo(ctx RequestContext, args []RespValue) {
	parsedArgs, e := GetArgsParser.Parse(args)
	if e != nil {
		ctx.SendError(e.Error())
		return
	}
	echo := parsedArgs.GetPos(0)
	ctx.SendResp(echo)
}
